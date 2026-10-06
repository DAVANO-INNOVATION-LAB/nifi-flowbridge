"""Exercise UI state transitions without claiming browser rendering coverage."""
import shutil
import subprocess
import unittest
from pathlib import Path

class UIContractTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node.js required for UI contract tests')
    def test_user_workflow_and_recovery(self):
        subprocess.run(['node', 'tests/ui-contract.cjs'], cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True, timeout=15)
