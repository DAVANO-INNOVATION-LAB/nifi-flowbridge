"""Generate native parser fixtures from the current exporter; never execute input."""
import json
from pathlib import Path
from flowbridge.media_targets import export_media
root = Path(__file__).resolve().parents[4]
folder = Path(__file__).parent / 'fixtures'
folder.mkdir(exist_ok=True)
blueprint = json.loads((root / 'examples/media-etl.json').read_text())
for target in ('camel-k', 'seatunnel'):
    for name, text in export_media(blueprint, target)['files'].items():
        if name.startswith(target + '/'):
            document = json.loads(text)
            if target == 'camel-k':
                document = document['spec']['flows']
            filename = ('camel-' if target == 'camel-k' else 'seatunnel-') + Path(name).name
            (folder / filename).write_text(json.dumps(document, indent=2) + '\n')
