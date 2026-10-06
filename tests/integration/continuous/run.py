"""Bounded local integration runner; requires cached Docker images, never pulls."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--docker-config')
    parser.add_argument('--context')
    parser.add_argument('--output', default='local-output/continuous-rerun')
    args = parser.parse_args()
    docker = ['docker']
    if args.docker_config: docker += ['--config', args.docker_config]
    if args.context: docker += ['--context', args.context]
    output = (ROOT / args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    def call(command, **kwargs):
        return subprocess.run(docker + command, cwd=ROOT, check=True, timeout=kwargs.pop('timeout', 300), **kwargs)
    with tempfile.TemporaryDirectory(prefix='flowbridge-continuous-') as temporary:
        envfile = Path(temporary) / 'credentials.env'
        envfile.write_text('CONTINUOUS_TEST_PASSWORD=' + secrets.token_urlsafe(36) + '\n')
        os.chmod(envfile, 0o600)
        compose = ['compose', '--env-file', str(envfile), '-f', 'tests/integration/continuous/compose.yaml']
        # Refuse to attach to an unrelated or still-running test project.
        existing = call(compose + ['ps', '-q'], capture_output=True, text=True).stdout.strip()
        if existing: raise RuntimeError('Existing continuous test containers must be cleaned up first')
        try:
            call(['build', '--network=none', '--pull=false', '-f', 'tests/integration/continuous/Dockerfile', '-t', 'flowbridge-continuous-smoke:local', '.'])
            call(compose + ['up', '-d', '--pull', 'never'])
            run = ['run', '--rm', '-i', '--network', 'container:flowbridge-continuous-nifi-1', '--memory', '384m', '--cpus', '1', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--env-file', str(envfile)]
            setup = call(run + ['--entrypoint', 'python', 'flowbridge-continuous-smoke:local', '/continuous/setup.py'], capture_output=True, timeout=220).stdout
            with (output / 'events.jsonl').open('wb') as events:
                call(run + ['flowbridge-continuous-smoke:local'], input=setup, stdout=events, timeout=260)
            rows = [json.loads(line) for line in (output / 'events.jsonl').read_text().splitlines() if line.startswith('{')]
            proof = next(row['proof'] for row in rows if 'proof' in row)
            assert proof['result'] == 'passed'
            native = next(row['native_export'] for row in rows if 'native_export' in row)
            (output / 'evidence.json').write_text(json.dumps(proof, indent=2) + '\n')
            (output / 'native-export.json').write_text(json.dumps(native, indent=2) + '\n')
            print(json.dumps({'result': 'passed', 'objects': proof['total_produced'], 'output': str(output)}))
        finally:
            call(compose + ['down', '--volumes'], timeout=90)

if __name__ == '__main__': main()
