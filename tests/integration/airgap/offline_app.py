"""Application assessment/package roundtrips with Docker --network none."""
import json
import socket
from flowbridge import service
from flowbridge.media import import_nifi_media
from flowbridge.media_targets import import_media_package

for address in ('1.1.1.1','8.8.8.8'):
    try:
        with socket.create_connection((address,443),timeout=1):pass
    except OSError:pass
    else:raise AssertionError('External connection unexpectedly allowed')
blueprint={'schema':'flowbridge/media-etl/v1','name':'offline-app-proof',
    's3':{'endpoint':'http://storage.internal:9090','region':'us-east-1','path_style_access':True},
    'kafka':{'brokers':'kafka.internal:9092'},
    'delivery':{'checkpoint_backend':'sqlite','deduplication':'bucket-key-version-etag','retries':3},
    'pipelines':[{'id':kind,'media_type':kind,'source':{'bucket':'demo-'+kind+'-in','prefix':''},
        'destination':{'bucket':'demo-'+kind+'-out','prefix':'processed/'},
        'stream':{'topic':'media-'+kind,'dead_letter_topic':'media-'+kind+'-dlq'},
        'processing':{'engine':'http','url':'https://processor.internal/process','method':'POST'}} for kind in ('image','text','video')]}
assert service.assess(blueprint)['report']['ok']
checks={}
for target in ('nifi','kafka','airflow','camel-k','seatunnel'):
    exported=service.convert(blueprint,target=target)
    assert exported['files']
    if target=='nifi': imported=import_nifi_media(json.loads(exported['files']['nifi-media-flow.json']))
    else: imported=import_media_package(exported['files'],target)
    assert imported['report']['ok'] and imported['blueprint']==blueprint
    checks[target]={'exported':True,'own_package_reimported':True,'native_etl_executed':False}
print(json.dumps({'result':'passed','network_mode':'none','service_assessment':True,'service_export_reimport_targets':checks,
                  'source_scope':'single_endpoint_synthetic_blueprint','external_tcp_probes_blocked':2,'dependency_downloads':False}))
