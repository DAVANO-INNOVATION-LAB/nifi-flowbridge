"""Modern source preparation is a draft, never a fabricated live checkpoint."""
import json
from pathlib import Path
import unittest
from flowbridge.service import convert

class NativeServiceTests(unittest.TestCase):
    def fixture(self):
        return json.loads((Path(__file__).parent/'integration/seatunnel_cutover/native-export.json').read_text())

    def test_modern_prepare_has_explicit_boundary_warning_and_no_offsets(self):
        result=convert(self.fixture(),'nifi','seatunnel',nifi_version='2')
        self.assertTrue(result['report']['ok'],result)
        self.assertEqual(result['report']['source'],'nifi')
        self.assertIn('live_boundary_required',{w['code'] for w in result['report']['warnings']})
        job=json.loads(result['files']['seatunnel.json'])
        self.assertNotIn('start_mode.offsets',job['source']['Kafka'])
        self.assertIn('does not deploy',result['files']['README.md'])
        self.assertNotIn('cutover_ready',result)

    def test_unknown_property_blocks_all_files(self):
        doc=self.fixture();doc['flowContents']['processors'][0]['properties']['unmapped.custom.transform']='never-copy'
        result=convert(doc,'nifi','seatunnel',nifi_version='2')
        self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})
        self.assertNotIn('never-copy',json.dumps(result))

    def test_wrong_source_or_explicit_future_version_blocks(self):
        for source,version in [('kafka','2'),('nifi','3'),('nifi','1')]:
            result=convert(self.fixture(),source,'seatunnel',nifi_version=version)
            self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})

    def test_embedded_future_version_blocks_auto_mode(self):
        doc=self.fixture();doc['nifiVersion']='3.0.0'
        result=convert(doc,'nifi','seatunnel')
        self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})

    def test_review_blocked_xml_cannot_bypass_modern_branch(self):
        doc=self.fixture();doc['_flowbridge_xml']={'export_blocked':True,'reason':'unknown legacy fields'}
        result=convert(doc,'nifi','seatunnel',nifi_version='2')
        self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})

    def test_downloaded_report_matches_displayed_warnings(self):
        result=convert(self.fixture(),'nifi','seatunnel',nifi_version='2')
        self.assertEqual(json.loads(result['files']['migration-report.json']),result['report'])

    def test_conflicting_embedded_source_version_blocks(self):
        doc=self.fixture();doc['nifiVersion']='1.28.0'
        result=convert(doc,'nifi','seatunnel')
        self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})
