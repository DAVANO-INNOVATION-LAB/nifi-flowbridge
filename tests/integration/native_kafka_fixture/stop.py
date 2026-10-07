"""Cleanup only the dedicated shared proof fixture after all targets finish."""
import subprocess
from pathlib import Path
root=Path(__file__).resolve().parents[3]
d=['docker','--config',str(root.parent/'local-workers/state/docker'),'--context','colima-dil']
subprocess.run(d+['rm','-f','flowbridge-native-kafka-helper'],check=False)
subprocess.run(d+['compose','--env-file',str(root/'local-output/native-kafka-fixture/test.env'),'-f','tests/integration/native_kafka_fixture/compose.yaml','down','--volumes'],cwd=root,check=True)

(root/'local-output/native-kafka-fixture/test.env').unlink(missing_ok=True)
