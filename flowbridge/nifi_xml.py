"""Bounded legacy NiFi XML inventory normalization; never executable migration.

Shapes verified against Apache NiFi support/nifi-1.x fixtures:
 nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-framework-core/
 src/test/resources/templates/template-0.7.0.xml
 src/test/resources/conf/standard-flow.xml
https://github.com/apache/nifi/tree/support/nifi-1.x

Original element data is preserved for review, including unknown fields. Callers
must block executable exports when _flowbridge_xml.export_blocked is true.
"""
import io
import re
import xml.etree.ElementTree as ET

MAX_BYTES = 2 * 1024 * 1024
MAX_ELEMENTS = 20000
MAX_DEPTH = 64


def _text(element):
    return '' if element is None or element.text is None else element.text


def _one(element, tag):
    found = element.findall(tag)
    if len(found) > 1:
        raise ValueError('Ambiguous duplicate singleton field in legacy XML.')
    return found[0] if found else None


def _value(element, tag, default=None):
    item = _one(element, tag)
    return default if item is None else _text(item)


def _raw(element):
    return {'tag': element.tag, 'attributes': dict(element.attrib),
            'text': element.text, 'tail': element.tail,
            'children': [_raw(child) for child in element]}


class _Normalizer:
    def __init__(self):
        self.unmapped = []

    def unknown(self, element, known, path):
        for index, child in enumerate(element):
            if child.tag not in known:
                self.unmapped.append(f'{path}/{child.tag}[{index}]')
        if element.attrib:
            self.unmapped.append(path + '/@attributes')

    def properties(self, element, path):
        result = {}
        entries = list(element.findall('property'))
        container = _one(element, 'properties')
        if container is not None:
            entries += list(container.findall('entry'))
            self.unknown(container, {'entry'}, path + '/properties')
        for index, entry in enumerate(entries):
            name = _value(entry, 'name') if entry.tag == 'property' else _value(entry, 'key')
            if not isinstance(name, str) or not name or name in result:
                raise ValueError('Missing or duplicate legacy XML property name.')
            value = _one(entry, 'value')
            if value is not None and list(value):
                self.unmapped.append(f'{path}/properties/{index}/structured-value')
                result[name] = _raw(value)
            elif value is not None and value.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}nil') == 'true':
                result[name] = None
            else:
                result[name] = None if value is None else _text(value)
            self.unknown(entry, {'name','key','value'}, f'{path}/properties/{index}')
        return result

    def component(self, element, path, kind):
        result = {'componentType': kind}
        aliases = {'id':'identifier','name':'name','parentGroupId':'groupIdentifier',
                   'class':'type','type':'type','comment':'comments','comments':'comments',
                   'state':'scheduledState','scheduledState':'scheduledState'}
        for old, new in aliases.items():
            value = _value(element, old)
            if value is not None:
                if new in result:
                    raise ValueError('Conflicting legacy XML aliases.')
                result[new] = value
        bundle = _one(element, 'bundle')
        if bundle is not None:
            result['bundle'] = {key: _value(bundle, key) for key in ('group','artifact','version') if _one(bundle,key) is not None}
            self.unknown(bundle, {'group','artifact','version'}, path + '/bundle')
        position = _one(element, 'position')
        if position is not None:
            result['position'] = {axis: position.attrib.get(axis, _value(position,axis)) for axis in ('x','y')}
        config = _one(element, 'config')
        settings = config if config is not None else element
        result['properties'] = self.properties(settings, path)
        fields = {'schedulingPeriod':'schedulingPeriod','schedulingStrategy':'schedulingStrategy',
                  'executionNode':'executionNode','penaltyDuration':'penaltyDuration',
                  'yieldDuration':'yieldDuration','bulletinLevel':'bulletinLevel',
                  'concurrentlySchedulableTaskCount':'concurrentlySchedulableTaskCount',
                  'maxConcurrentTasks':'concurrentlySchedulableTaskCount','runDurationMillis':'runDurationMillis'}
        integers = {'concurrentlySchedulableTaskCount','runDurationMillis'}
        for old, new in fields.items():
            value = _value(settings, old)
            if value is not None:
                if new in result:
                    raise ValueError('Conflicting legacy scheduling fields.')
                if new in integers:
                    try: value = int(value)
                    except ValueError: raise ValueError('Invalid legacy scheduling number.') from None
                result[new] = value
        if config is not None:
            self.unknown(config, set(fields) | {'properties','property'}, path + '/config')
        relationships = element.findall('autoTerminatedRelationship') + element.findall('autoTerminatedRelationships')
        if relationships:
            result['autoTerminatedRelationships'] = [_text(item) for item in relationships]
        self.unknown(element, set(aliases) | set(fields) | {'bundle','position','properties','property','config','autoTerminatedRelationship','autoTerminatedRelationships'}, path)
        return result

    def connection(self, element, path):
        result = {'componentType':'CONNECTION'}
        for key, target in [('id','identifier'),('parentGroupId','groupIdentifier'),('name','name'),
                            ('flowFileExpiration','flowFileExpiration'),('backPressureDataSizeThreshold','backPressureDataSizeThreshold')]:
            value = _value(element,key)
            if value is not None: result[target] = value
        value = _value(element,'backPressureObjectThreshold')
        if value is not None:
            try: result['backPressureObjectThreshold'] = int(value)
            except ValueError: raise ValueError('Invalid legacy connection threshold.') from None
        for end in ('source','destination'):
            nested = _one(element,end)
            if nested is not None:
                result[end] = {key:_value(nested,key) for key in ('id','groupId','type') if _one(nested,key) is not None}
                self.unknown(nested, {'id','groupId','type'}, path+'/'+end)
                if any(_one(element,end+suffix) is not None for suffix in ('Id','GroupId','Type')):
                    raise ValueError('Ambiguous legacy connection endpoints.')
            else:
                result[end] = {key:_value(element,end+suffix) for key,suffix in [('id','Id'),('groupId','GroupId'),('type','Type')] if _one(element,end+suffix) is not None}
        result['selectedRelationships'] = [_text(item) for item in element if item.tag in ('relationship','selectedRelationships')]
        known = {'id','parentGroupId','name','flowFileExpiration','backPressureDataSizeThreshold','backPressureObjectThreshold','source','destination','relationship','selectedRelationships'}
        known |= {end+suffix for end in ('source','destination') for suffix in ('Id','GroupId','Type')}
        self.unknown(element,known,path)
        return result

    def group(self, element, path):
        result = {key:[] for key in ('processors','processGroups','connections','controllerServices','inputPorts','outputPorts','funnels','remoteProcessGroups','labels')}
        for old,new in [('id','identifier'),('name','name'),('parentGroupId','groupIdentifier'),('comment','comments'),('comments','comments')]:
            value = _value(element,old)
            if value is not None:
                if new in result: raise ValueError('Conflicting legacy group aliases.')
                result[new] = value
        mapping = {'processor':('processors','PROCESSOR'),'processors':('processors','PROCESSOR'),
                   'controllerService':('controllerServices','CONTROLLER_SERVICE'),'controllerServices':('controllerServices','CONTROLLER_SERVICE'),
                   'inputPort':('inputPorts','INPUT_PORT'),'inputPorts':('inputPorts','INPUT_PORT'),
                   'outputPort':('outputPorts','OUTPUT_PORT'),'outputPorts':('outputPorts','OUTPUT_PORT'),
                   'funnel':('funnels','FUNNEL'),'funnels':('funnels','FUNNEL')}
        contents = _one(element,'contents')
        container = contents if contents is not None else element
        for index,child in enumerate(container):
            child_path = path+'/'+child.tag+f'[{index}]'
            if child.tag in mapping:
                collection,kind = mapping[child.tag]
                wrapped = child.findall('controllerService') if child.tag == 'controllerServices' else []
                if wrapped:
                    for service_index, service in enumerate(wrapped):
                        result[collection].append(self.component(service, child_path+f'/controllerService[{service_index}]',kind))
                    self.unknown(child, {'controllerService'}, child_path)
                else:
                    result[collection].append(self.component(child,child_path,kind))
            elif child.tag in ('processGroup','processGroups'):
                result['processGroups'].append(self.group(child,child_path))
            elif child.tag in ('connection','connections'):
                result['connections'].append(self.connection(child,child_path))
            elif child.tag in ('remoteProcessGroup','remoteProcessGroups','label','labels'):
                collection = 'labels' if child.tag in ('label','labels') else 'remoteProcessGroups'
                result[collection].append(_raw(child))
                self.unmapped.append(child_path)
        known = set(mapping) | {'processGroup','processGroups','connection','connections','id','name','parentGroupId','comment','comments','contents'}
        self.unknown(element,known,path)
        if contents is not None: self.unknown(contents,known,path+'/contents')
        return result


def parse_nifi_xml(data):
    """Normalize UTF-8 legacy XML for inventory; retain raw tree and block export.

    Gzip decoding is the caller's bounded responsibility. Never parses entities,
    follows include URLs, decrypts properties, executes code or modifies input.
    """
    if isinstance(data,str): data=data.encode('utf-8')
    if not isinstance(data,bytes) or not 0<len(data)<=MAX_BYTES:
        raise ValueError('Legacy XML must be at most 2 MiB.')
    try: source=data.decode('utf-8-sig')
    except UnicodeDecodeError: raise ValueError('Legacy XML must use UTF-8 encoding.') from None
    if '\x00' in source or re.search(r'<!\s*(DOCTYPE|ENTITY)\b',source,re.I):
        raise ValueError('DTD and entity declarations are prohibited.')
    declaration=re.match(r'\s*<\?xml\s+[^?]*encoding\s*=\s*[\"\']([^\"\']+)',source,re.I)
    if declaration and declaration.group(1).lower() not in ('utf-8','utf8','us-ascii'):
        raise ValueError('Legacy XML must use UTF-8 encoding.')
    try:
        depth=count=0
        parser=ET.iterparse(io.StringIO(source),events=('start','end'))
        for event,element in parser:
            if event=='start':
                depth+=1; count+=1
                if depth>MAX_DEPTH or count>MAX_ELEMENTS: raise ValueError('Legacy XML exceeds structure limits.')
                if not isinstance(element.tag,str) or element.tag.startswith('{'): raise ValueError('Namespaced legacy XML elements are not supported.')
            else: depth-=1
        root=parser.root
    except ET.ParseError: raise ValueError('Malformed legacy NiFi XML.') from None
    normalizer=_Normalizer()
    global_services=[]
    if root.tag=='template':
        snippet=_one(root,'snippet')
        if snippet is None: raise ValueError('NiFi template requires a snippet.')
        group=normalizer.group(snippet,'/template/snippet')
        group['name']=_value(root,'name','Imported legacy template')
        group.setdefault('identifier',_value(root,'id','legacy-template-root'))
        normalizer.unknown(root,{'snippet','name','id','description'},'/template')
        source_format='nifi-template-xml'
    elif root.tag=='flowController':
        root_group=_one(root,'rootGroup')
        if root_group is None: raise ValueError('NiFi flowController requires a rootGroup.')
        group=normalizer.group(root_group,'/flowController/rootGroup')
        services=_one(root,'controllerServices')
        if services is not None:
            for index, service in enumerate(services.findall('controllerService')):
                global_services.append(normalizer.component(service,f'/flowController/controllerServices/controllerService[{index}]','CONTROLLER_SERVICE'))
            normalizer.unknown(services,{'controllerService'},'/flowController/controllerServices')
        normalizer.unknown(root,{'rootGroup','controllerServices'},'/flowController')
        source_format='nifi-flow-controller-xml'
    else: raise ValueError('Expected a NiFi template or flowController XML root.')
    return {'flowContents':group, **({'controllerServices':global_services} if global_services else {}), '_flowbridge_xml':{'source_format':source_format,
            'review_required':True,'export_blocked':True,'unmapped_paths':normalizer.unmapped,
            'original_tree':_raw(root),
            'notice':'Legacy XML normalization is an inventory aid. Review original fields and validate mappings before any executable export.'}}
