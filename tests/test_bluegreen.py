import copy
import json
from pathlib import Path
import tempfile
import unittest
from flowbridge.bluegreen import BlueGreenPlan
from flowbridge.nifi_s3 import analyze_nifi_s3, MigrationError
from test_nifi_s3 import native
from test_media_runtime import S3

class Fence:
    def __init__(self): self.stopped = False
    def fence(self, profile):
        self.stopped = True
        return {'pipeline_ids': [l['id'] for l in profile['lanes']], 'source_stopped': True, 'queued_flowfiles': 0, 'active_threads': 0, 'fence_id': 'test-server-readback'}
    def verify(self, profile, proof): return self.stopped

class BlueGreenTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.profile = analyze_nifi_s3(native())['profile']
        self.profile['schema'] = 'flowbridge/continuous-s3-fleet/v1'
        self.clients = {name: S3() for name in ('source', 'target', 'green')}
        for client in self.clients.values():
            original = client.head_object
            client.head_object = lambda Bucket, Key, c=client, old=original: {**old(Bucket=Bucket, Key=Key), 'Metadata': c.objects[(Bucket, Key)][1].get('Metadata', {})}
        self.factory = lambda c: self.clients[c['endpoint'].split('//')[1].split(':')[0]]
        self.mapping = {l['id']: {**l['destination'], 'endpoint': 'http://green:9090', 'bucket': l['destination']['bucket'] + '-green'} for l in self.profile['lanes']}
        self.fence = Fence()
        self.path = Path(self.temp.name) / 'plan.sqlite'
        self.plan = BlueGreenPlan(self.profile, self.path, self.factory, self.fence)
        for lane in self.profile['lanes']:
            for side in ('source', 'destination'):
                client = self.clients['source' if side == 'source' else 'target']
                client.put_object(Bucket=lane[side]['bucket'], Key='sample', Body=b'payload', Metadata={'media_type': lane['media_type']})
    def ready(self):
        self.plan.shadow_once(self.mapping)
        return self.plan.validate_shadow()
    def test_shadow_compares_every_pipeline_and_persists(self):
        result = self.ready(); self.assertEqual(result['phase'], 'ready')
        self.assertEqual(result['validation']['object_count'], 3)
        restart = BlueGreenPlan(self.profile, self.path, self.factory)
        self.assertEqual(restart.status()['validation'], result['validation'])
        self.assertFalse(result['production_changed'])
    def test_missing_blue_blocks_even_if_green_matches(self):
        self.clients['target'].objects.clear()
        self.assertEqual(self.ready()['phase'], 'blocked')
    def test_no_adapter_blocks_cutover_and_browser_bool_not_accepted(self):
        self.ready(); self.plan.fence_adapter = None
        self.assertEqual(self.plan.request_cutover()['phase'], 'blocked')
        with self.assertRaises(TypeError): self.plan.request_cutover(True)
    def test_real_ownership_protocol_then_later_arrivals_and_restart(self):
        self.ready(); result = self.plan.request_cutover()
        self.assertEqual(result['phase'], 'active'); self.assertTrue(self.fence.stopped)
        lane = self.profile['lanes'][0]
        self.clients['source'].put_object(Bucket=lane['source']['bucket'], Key='later', Body=b'new')
        self.assertEqual(self.plan.run_production_once()['production_result']['counts']['processed'], 1)
        restart = BlueGreenPlan(self.profile, self.path, self.factory, self.fence)
        self.assertEqual(restart.run_production_once()['production_result']['counts']['processed'], 0)
        self.fence.stopped = False
        with self.assertRaises(MigrationError): restart.run_production_once()
    def test_invalid_partial_fence_never_grants_ownership(self):
        self.ready()
        self.fence.fence = lambda profile: {'pipeline_ids': [], 'source_stopped': True}
        with self.assertRaises(MigrationError): self.plan.request_cutover()
        self.assertFalse(self.plan.status()['production_changed'])
    def test_overlap_rejected_and_fingerprint_immutable(self):
        mapping = {l['id']: l['destination'] for l in self.profile['lanes']}
        with self.assertRaises(MigrationError): self.plan.shadow_once(mapping)
        changed = copy.deepcopy(self.profile); changed['lanes'][0]['source']['prefix'] = 'changed/'
        with self.assertRaises(MigrationError): BlueGreenPlan(changed, self.path, self.factory)

    def test_sixty_pipeline_watermark_and_shadow_isolation(self):
        lanes = []
        for index in range(60):
            lane = copy.deepcopy(self.profile['lanes'][index % 3])
            lane['id'] = 'pipeline-' + str(index)
            lane['source']['bucket'] = 'source-' + str(index)
            lane['destination']['bucket'] = 'target-' + str(index)
            lanes.append(lane)
            self.clients['source'].put_object(Bucket=lane['source']['bucket'], Key='sample', Body=b'fleet')
            self.clients['target'].put_object(Bucket=lane['destination']['bucket'], Key='sample', Body=b'fleet', Metadata={'media_type': lane['media_type']})
        profile = {**self.profile, 'lanes': lanes}
        mapping = {l['id']: {**l['destination'], 'endpoint': 'http://green:9090', 'bucket': 'green-' + str(i)} for i, l in enumerate(lanes)}
        plan = BlueGreenPlan(profile, Path(self.temp.name) / 'fleet.sqlite', self.factory)
        blue_puts = self.clients['target'].puts
        plan.shadow_once(mapping)
        status = plan.validate_shadow()
        self.assertEqual(status['phase'], 'ready')
        self.assertEqual(status['validation']['pipeline_count'], 60)
        self.assertEqual(status['validation']['object_count'], 60)
        self.assertEqual(self.clients['target'].puts, blue_puts)

    def test_shadow_and_validation_refused_after_green_ownership(self):
        self.ready();self.plan.request_cutover()
        before=self.plan.status()
        for action in (lambda:self.plan.shadow_once(self.mapping),self.plan.validate_shadow):
            with self.assertRaisesRegex(MigrationError,'shadow_actions_refused'):action()
        self.assertEqual(self.plan.status()['ownership'],'green')
        self.assertEqual(self.plan.status()['phase'],before['phase'])
        self.assertTrue(self.plan.status()['production_changed'])

    def test_repeated_cutover_is_idempotent_and_cannot_clear_ownership(self):
        self.ready();self.plan.request_cutover()
        def forbidden(profile):raise AssertionError('must not re-fence')
        self.fence.fence=forbidden
        restarted=BlueGreenPlan(self.profile,self.path,self.factory,self.fence)
        self.assertEqual(restarted.request_cutover()['phase'],'active')
        restarted.fence_adapter=None
        result=restarted.request_cutover();self.assertTrue(result['production_changed']);self.assertEqual(result['ownership'],'green');self.assertFalse(result['cutover_supported'])

    def test_adapter_capability_reflects_current_restart_configuration(self):
        self.assertTrue(self.plan.status()['cutover_supported'])
        without=BlueGreenPlan(self.profile,self.path,self.factory)
        self.assertFalse(without.status()['cutover_supported'])
        with_adapter=BlueGreenPlan(self.profile,self.path,self.factory,self.fence)
        self.assertTrue(with_adapter.status()['cutover_supported'])

    def test_failed_fence_requires_operator_review_and_blocks_shadow(self):
        self.ready()
        def fail(profile):raise RuntimeError('fence transport failed')
        self.fence.fence=fail
        with self.assertRaises(MigrationError):self.plan.request_cutover()
        restart=BlueGreenPlan(self.profile,self.path,self.factory,self.fence)
        with self.assertRaisesRegex(MigrationError,'operator_review'):restart.request_cutover()
        with self.assertRaisesRegex(MigrationError,'shadow_actions_refused'):restart.shadow_once(self.mapping)
        self.assertFalse(restart.status()['production_changed'])

    def test_changed_validated_source_blocks_before_fencing(self):
        self.ready();lane=self.profile['lanes'][0]
        self.clients['source'].put_object(Bucket=lane['source']['bucket'],Key='sample',Body=b'changed')
        with self.assertRaisesRegex(MigrationError,'boundary_changed'):self.plan.request_cutover()
        self.assertFalse(self.fence.stopped);self.assertFalse(self.plan.status()['production_changed'])

    def test_change_during_fence_blocks_ownership(self):
        self.ready();original=self.fence.fence;lane=self.profile['lanes'][0]
        def mutate(profile):
            proof=original(profile)
            self.clients['source'].put_object(Bucket=lane['source']['bucket'],Key='sample',Body=b'changed')
            return proof
        self.fence.fence=mutate
        with self.assertRaises(MigrationError):self.plan.request_cutover()
        self.assertNotEqual(self.plan.status().get('ownership'),'green')
        self.assertTrue(self.plan.status()['cutover_attempted'])

    def test_new_keys_after_validation_are_backfilled_without_losing_boundary(self):
        self.ready();lane=self.profile['lanes'][0]
        self.clients['source'].put_object(Bucket=lane['source']['bucket'],Key='new-arrival',Body=b'new')
        result=self.plan.request_cutover();self.assertEqual(result['phase'],'active')
        self.assertEqual(result['baseline']['pending_backfill'],1)
        self.assertEqual(result['production_result']['counts']['processed'],1)
        self.assertIn('new_keys',result['data_boundary'])

    def test_expired_validation_requires_fresh_assessment(self):
        from unittest.mock import patch
        self.ready()
        observed=self.plan.status()['validation']['observed_at']
        with patch('flowbridge.bluegreen.time.time',return_value=observed+301):
            with self.assertRaisesRegex(MigrationError,'validation_expired'):self.plan.request_cutover()
        self.assertFalse(self.fence.stopped)
