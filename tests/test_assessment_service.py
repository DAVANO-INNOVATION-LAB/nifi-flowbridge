import json
import unittest
from pathlib import Path
from flowbridge.service import assess, convert

class AssessmentServiceTests(unittest.TestCase):
 def fixture(self):
  return json.loads((Path(__file__).parent.parent/'examples/nifi-flow.json').read_text())
 def test_inventory_handles_more_than_two_nodes_without_claiming_conversion(self):
  data=self.fixture(); nodes=data['flowContents']['processors'];third=dict(nodes[1],identifier='third',name='Extra processor');nodes.append(third)
  result=assess(data,'nifi','1')
  self.assertEqual(len(result['assessment']['graph']['processors']),3)
  self.assertFalse(result['assessment']['capabilities']['automatic_conversion'])
  self.assertFalse(convert(data,'nifi','seatunnel')['report']['ok'])
 def test_unknown_version_is_not_certified(self):
  result=assess(self.fixture(),'nifi','3')
  self.assertFalse(result['report']['ok'])
 def test_future_version_blocks_legacy_and_upgrade_exports(self):
  for target in ("nifi", "seatunnel", "nifi-upgrade"):
   self.assertFalse(convert(self.fixture(),"nifi",target,nifi_version="3")["report"]["ok"])
 def test_assessment_does_not_return_password(self):
  data=self.fixture();data['flowContents']['processors'][0]['properties']['password']='never-echo-this-value'
  result=assess(data,'nifi','1')
  self.assertNotIn('never-echo-this-value',json.dumps(result))
 def test_url_query_credentials_are_redacted(self):
  data=self.fixture();data["flowContents"]["processors"][0]["properties"]["HTTP URL"]="https://example.test/?api_key=private-query-value"
  self.assertNotIn("private-query-value", json.dumps(assess(data,"nifi")))
 def test_upgrade_plan_is_review_only(self):
  result=convert(self.fixture(),'nifi','nifi-upgrade')
  plan=json.loads(result['files']['nifi-upgrade-plan.json'])
  self.assertFalse(plan['ready_to_deploy'])
  self.assertTrue(result['report']['warnings'])
 def test_other_source_cannot_be_claimed_as_airflow(self):
  result=convert({'schema':'flowbridge/v1'},'flowbridge','airflow')
  self.assertFalse(result['report']['ok'])
