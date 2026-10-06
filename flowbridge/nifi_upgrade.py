"""Review-only NiFi upgrade rules; never claim full runtime compatibility.

Rule tables adapted from stackabletech/nifi-migrate at
0de82df8ee73a9b2c52bd6c28850cab37fb93eff (Apache-2.0).
SPDX-FileCopyrightText: 2025 Stackable GmbH
SPDX-License-Identifier: Apache-2.0
Python planning, conflict detection and JSON Patch generation added by Flowbridge.
"""
import hashlib
import json

UPSTREAM = 'https://github.com/stackabletech/nifi-migrate/tree/0de82df8ee73a9b2c52bd6c28850cab37fb93eff'
JOLT_JSON = {'jolt-spec':'Jolt Specification','jolt-transform':'Jolt Transform','pretty_print':'Pretty Print','jolt-custom-class':'Custom Transformation Class Name','jolt-custom-modules':'Custom Module Directory'}
JOLT_RECORD = {'jolt-record-transform':'Jolt Transform','jolt-record-spec':'Jolt Specification','jolt-record-custom-class':'Custom Transformation Class Name','jolt-record-custom-modules':'Custom Module Directory','jolt-record-transform-cache-size':'Transform Cache Size'}
RULES = {
 'org.apache.nifi.processors.standard.JoltTransformJSON': ('org.apache.nifi.processors.jolt.JoltTransformJSON','nifi-standard-nar','nifi-jolt-nar',JOLT_JSON),
 'org.apache.nifi.processors.jolt.record.JoltTransformRecord': ('org.apache.nifi.processors.jolt.JoltTransformRecord','nifi-jolt-record-nar','nifi-jolt-nar',JOLT_RECORD),
}
for package, names in [('client',['MapCacheClientService','SetCacheClientService']),('server.map',['MapCacheServer']),('server.set',['SetCacheServer'])]:
 for name in names:
  RULES[f'org.apache.nifi.distributed.cache.{package}.Distributed{name}'] = (f'org.apache.nifi.distributed.cache.{package}.{name}',None,None,{})


def pointer(key):
 return str(key).replace('~','~0').replace('/','~1')


def plan_upgrade(document):
 """Return reviewable JSON Patch operations, never a deployment-ready flow.

No source values are included except recognized static class/bundle names. Property
renames use move operations. Test operations guard applying patches to a changed
source. Conflicting old/new keys block all patch output instead of overwriting.
 """
 changes, errors, inventory = [], [], []
 def visit(value, path='', depth=0):
  if depth>64:
   raise ValueError('Flow nesting exceeds the upgrade planner limit.')
  if isinstance(value, list):
   for index,item in enumerate(value): visit(item,path+'/'+str(index),depth+1)
  elif isinstance(value,dict):
   kind=value.get('type')
   component=path.rsplit('/',2)[-2] if path.count('/')>=2 else ''
   if component in ('processors','controllerServices') and isinstance(kind,str):
    rule=RULES.get(kind)
    inventory.append({'path':path,'type':kind,'rule_available':bool(rule)})
    if rule:
     new,old_bundle,new_bundle,props=rule
     operations=[{'op':'test','path':path+'/type','value':kind},{'op':'replace','path':path+'/type','value':new}]
     bundle=value.get('bundle',{})
     if isinstance(bundle,dict) and bundle.get('group') not in (None, 'org.apache.nifi'):
      errors.append({'code':'upgrade.vendor_bundle','message':f'Custom vendor bundle at {path} requires manual migration.'})
     if old_bundle:
      if not isinstance(bundle,dict) or bundle.get('artifact')!=old_bundle:
       errors.append({'code':'upgrade.bundle_conflict','message':f'Unexpected bundle for component at {path}; no patch can be generated.'})
      else:
       operations.extend([{'op':'test','path':path+'/bundle/artifact','value':old_bundle},{'op':'replace','path':path+'/bundle/artifact','value':new_bundle}])
     for container in ('properties','propertyDescriptors'):
      settings=value.get(container,{})
      if not isinstance(settings,dict):
       errors.append({'code':'upgrade.invalid_properties','message':f'Invalid property object at {path}.'});continue
      for old,new_key in props.items():
       if old not in settings:continue
       if new_key in settings:
        errors.append({'code':'upgrade.property_conflict','message':f'Both legacy and renamed property exist at {path}; resolve the conflict explicitly.'});continue
       base=path+'/'+container+'/'
       operations.append({'op':'move','from':base+pointer(old),'path':base+pointer(new_key)})
       if container=='propertyDescriptors' and isinstance(settings[old],dict):
        for label in ('name','displayName'):
         operations.append({'op':'add','path':base+pointer(new_key)+'/'+label,'value':new_key})
     changes.append({'component_path':path,'rule':kind,'operations':operations})
   for key,item in value.items(): visit(item,path+'/'+pointer(key),depth+1)
 visit(document)
 return {'source_sha256':hashlib.sha256(json.dumps(document,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest(),'schema':'flowbridge/nifi-upgrade-plan/v1','target_family':'2.x','ready_to_deploy':False,'source':UPSTREAM,'changes':[] if errors else changes,'errors':errors,'inventory':inventory,'required_reviews':['Before applying any operation, verify the source_sha256 against the canonical JSON input; a changed document requires a new plan.','Select and verify target bundle versions; this planner deliberately does not invent them.','Validate all unchanged processors, services, properties, parameters and runtime semantics on the target NiFi release.','Apply a reviewed patch to a copy only; this plan does not stop processors, transfer state or deploy a flow.']}
