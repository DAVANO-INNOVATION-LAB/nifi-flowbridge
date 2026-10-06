import copy
import json
import unittest
from flowbridge.core import analyze, convert, FORMATS

EXAMPLE = {'schema':'flowbridge/v1','name':'kafka-copy','source':{'type':'kafka','brokers':'localhost:9092','topic':'events-in','group':'flowbridge-demo','offset':'earliest'},'sink':{'type':'kafka','brokers':'localhost:9092','topic':'events-out'}}

class CoreTests(unittest.TestCase):
    def test_all_formats_roundtrip(self):
        for target in FORMATS:
            with self.subTest(target=target):
                result=convert(EXAMPLE,target=target)
                self.assertTrue(result['report']['ok'],result)
                artifact=json.loads(next(v for k,v in result['files'].items() if k!='migration-report.json'))
                restored=analyze(artifact)
                self.assertTrue(restored['report']['ok'],restored)
                self.assertEqual(restored['flow'],EXAMPLE)

    def native(self,target):
        result=convert(EXAMPLE,target=target)
        return json.loads(next(v for k,v in result['files'].items() if k!='migration-report.json'))

    def blocked(self,data,source='auto'):
        result=convert(data,source,'kafka')
        self.assertFalse(result['report']['ok'])
        self.assertEqual(result['files'],{})
        return result

    def test_secret_not_echoed(self):
        data=copy.deepcopy(EXAMPLE)
        data['source']['sasl.password']='DO_NOT_LEAK_THIS_PASSWORD'
        result=self.blocked(data)
        self.assertNotIn('DO_NOT_LEAK',json.dumps(result))

    def test_nested_secrets(self):
        data=self.native('nifi')
        data['parameterContexts']={'context':{'password':'DO_NOT_LEAK'}}
        self.assertNotIn('DO_NOT_LEAK',json.dumps(self.blocked(data)))

    def test_unknown_processor(self):
        data=self.native('nifi')
        data['flowContents']['processors'][0]['type']='org.apache.nifi.ExecuteScript'
        self.blocked(data)

    def test_record_serialization_cannot_be_dropped(self):
        data=self.native('nifi')
        data['flowContents']['processors'][0]['properties']['Record Reader']='service-id'
        self.blocked(data)

    def test_failure_relationship(self):
        data=self.native('nifi')
        data['flowContents']['connections'][0]['selectedRelationships']=['failure']
        self.blocked(data)

    def test_disconnected_or_duplicate_nodes(self):
        for duplicate in (False,True):
            data=self.native('nifi')
            if duplicate:
                data['flowContents']['processors'][1]['identifier']=data['flowContents']['processors'][0]['identifier']
            else:
                data['flowContents']['connections'][0]['destination']['id']='unknown'
            self.blocked(data)

    def test_nested_group(self):
        data=self.native('nifi')
        data['flowContents']['processGroups']=[{'name':'untranslated'}]
        self.blocked(data)

    def test_camel_transform(self):
        data=self.native('camel-k')
        data['spec']['flows'][0]['from']['steps'].insert(0,{'log':'x'})
        self.blocked(data)

    def test_seatunnel_transform(self):
        data=self.native('seatunnel')
        data['transform']={'Sql':{'query':'select * from events'}}
        self.blocked(data)

    def test_seatunnel_lossy_format(self):
        data=self.native('seatunnel')
        data['source']['Kafka']['format']='text'
        self.blocked(data)

    def test_expressions_and_credentials(self):
        for value in ('${broker}', '#{broker}', '{{broker}}', 'https://me:password@host:9092'):
            data=copy.deepcopy(EXAMPLE)
            data['source']['brokers']=value
            self.blocked(data)

    def test_invalid_shapes_never_crash(self):
        for data in (None,[],{},'public class Main {}', {'flowContents':{'processors':[None,None],'connections':[None]}}, {'schema':'flowbridge/v1','name': [],'source':None,'sink':{}}):
            self.blocked(data)

    def test_deep_input(self):
        data={}
        root=data
        for i in range(50):
            data['x']={}
            data=data['x']
        self.blocked(root)

    def test_feedback_loop(self):
        data=copy.deepcopy(EXAMPLE)
        data['sink']['topic']=data['source']['topic']
        self.blocked(data)

    def test_service_references_require_manual_mapping(self):
        data=self.native('nifi')
        data['externalControllerServices']={'id':{'name':'Kafka connection'}}
        self.blocked(data)

    def test_custom_concurrency_rejected(self):
        data=self.native('nifi')
        data['flowContents']['processors'][0]['concurrentlySchedulableTaskCount']=4
        self.blocked(data)

    def test_config_scheduling_aliases_are_validated(self):
        for field,value in (('runDurationMillis',10),('concurrentlySchedulableTaskCount',4),('schedulingPeriod','5 sec')):
            data=self.native('nifi')
            node=data['flowContents']['processors'][0]
            node['config']={'properties':node.pop('properties'),field:value}
            self.blocked(data)

    def test_conflicting_config_aliases_rejected(self):
        data=self.native('nifi')
        data['flowContents']['processors'][0]['config']={'properties':{'topic':'different'}}
        self.blocked(data)

    def test_camel_name_must_be_kubernetes_compatible(self):
        data=copy.deepcopy(EXAMPLE)
        data['name']='Upper_Case.Name'
        result=convert(data,target='camel-k')
        self.assertFalse(result['report']['ok'])
        self.assertEqual(result['files'],{})

    def test_deterministic_artifacts(self):
        for target in FORMATS:
            self.assertEqual(convert(EXAMPLE,target=target),convert(EXAMPLE,target=target))

    def test_unknown_target(self):
        result=convert(EXAMPLE,target='shell')
        self.assertFalse(result['report']['ok'])
        self.assertFalse(result['files'])

    def test_nifi_rootgroup_and_config_properties(self):
        data=self.native('nifi')
        group=data['flowContents']
        for node in group['processors']:
            node['id']=node.pop('identifier')
            node['config']={'properties':node.pop('properties')}
        self.assertEqual(analyze({'rootGroup':group})['flow'],EXAMPLE)

if __name__=='__main__':
    unittest.main()
