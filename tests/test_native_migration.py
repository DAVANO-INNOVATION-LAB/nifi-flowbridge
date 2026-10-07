import json
from pathlib import Path
import unittest
from unittest.mock import patch
from flowbridge.live.native import dispatch
from flowbridge.live.platforms import PlatformError
from flowbridge.service import convert

ROOT=Path(__file__).resolve().parents[1]
class NativeMigrationTests(unittest.TestCase):
    def payload(self, target='airflow'):
        return {'target':target,'source':{'platform':'nifi','url':'https://nifi.example/nifi-api','group_id':'root'},'destination':{'url':'https://airflow.example/api/v2','resource':'dag-one','namespace':'default'}}
    def test_no_mutation_operation(self):
        with patch('flowbridge.live.native.NiFiClient') as nifi,patch('flowbridge.live.native.connection'):
            with self.assertRaises(PlatformError):dispatch('cutover',self.payload())
            nifi.return_value.export_flow.assert_not_called()
    def test_inspect_airflow_is_not_approval(self):
        with patch('flowbridge.live.native.NiFiClient'),patch('flowbridge.live.native.connection') as connect:
            connect.return_value.request.return_value={'dag_id':'dag-one','is_paused':False}
            result=dispatch('inspect',self.payload())
            self.assertFalse(result['cutover_ready'])
            self.assertFalse(result['destination']['paused'])
            connect.return_value.request.assert_called_once_with('GET','/dags/dag-one')
    def test_inspect_airflow_rejects_wrong_resource(self):
        with patch('flowbridge.live.native.NiFiClient'),patch('flowbridge.live.native.connection') as connect:
            connect.return_value.request.return_value={'dag_id':'other','is_paused':True}
            with self.assertRaises(PlatformError):dispatch('inspect',self.payload())
    def test_source_required(self):
        payload=self.payload();payload['source']['platform']='kafka'
        with self.assertRaises(PlatformError):dispatch('prepare',payload)
    def test_airflow_native_package_requires_contract(self):
        document=json.loads((ROOT/'examples/nifi-continuous-media.json').read_text())
        result=convert(document,'nifi','airflow-s3')
        self.assertFalse(result['report']['ok'])
        result=convert(document,'nifi','airflow-s3',batch_contract={'mode':'scheduled_microbatch','acknowledge_scheduling_change':True})
        self.assertTrue(result['report']['ok']);self.assertIn('dags/flowbridge_s3.py',result['files'])
        result=convert(document,'nifi','airflow-s3',nifi_version='3')
        self.assertFalse(result['report']['ok'])
    def test_connected_prepare_never_claims_deployment(self):
        document=json.loads((ROOT/'examples/nifi-continuous-media.json').read_text())
        with patch('flowbridge.live.native.NiFiClient') as nifi,patch('flowbridge.live.native.connection'):
            nifi.return_value.export_flow.return_value=document
            nifi.return_value.inspect.return_value={'active_threads':0,'queued_count':0}
            payload=self.payload();payload['batch_contract']={'mode':'scheduled_microbatch','acknowledge_scheduling_change':True}
            result=dispatch('prepare',payload)
            self.assertTrue(result['summary']['prepared'])
            self.assertFalse(result['summary']['deployed']);self.assertFalse(result['cutover_ready'])
            self.assertEqual(result['export_target'],'airflow-s3')

    def test_fence_requires_explicit_authorization_before_controller(self):
        with patch('flowbridge.live.native.NiFiClient'),patch('flowbridge.live.native.connection'),patch('flowbridge.live.nifi_fence.NiFiFence') as controller:
            with self.assertRaisesRegex(PlatformError,'Explicit authorization'):dispatch('fence',self.payload())
            controller.assert_not_called()

    def test_fence_binds_mapping_callback_and_returns_server_id(self):
        document=json.loads((ROOT/'examples/nifi-continuous-media.json').read_text())
        with patch('flowbridge.live.native.NiFiClient') as nifi,patch('flowbridge.live.native.connection'),patch('flowbridge.live.nifi_fence.NiFiFence') as controller:
            nifi.return_value.export_flow.return_value=document
            nifi.return_value.inspect.return_value={}
            payload=self.payload();payload.update(acknowledge=True,batch_contract={'mode':'scheduled_microbatch','acknowledge_scheduling_change':True})
            def stop(group,timeout,validate_scope):
                self.assertTrue(validate_scope('actual-group'))
                return {'id':'server-proof','group_id':'actual-group','graph':{'processors':{'p':'type'}},'queued_flowfiles':0,'active_threads':0,'cutover_ready':False}
            controller.return_value.stop_and_drain.side_effect=stop
            result=dispatch('fence',payload)
            self.assertEqual(result['proof_id'],'server-proof');self.assertFalse(result['cutover_ready'])
            nifi.return_value.export_flow.assert_called_once_with('actual-group')

    def test_failed_fence_warns_about_partial_source_stop(self):
        with patch('flowbridge.live.native.NiFiClient'),patch('flowbridge.live.native.connection'),patch('flowbridge.live.nifi_fence.NiFiFence') as controller:
            controller.return_value.stop_and_drain.side_effect=PlatformError('drain_timeout','Deadline exceeded.')
            payload=self.payload();payload['acknowledge']=True
            with self.assertRaisesRegex(PlatformError,'partially stopped'):dispatch('fence',payload)
