"""Bounded native Camel K S3 route generation from reviewed native NiFi flows."""
import hashlib
import json
from pathlib import Path
from urllib.parse import urlencode
from ..fleet import discover_fleet


def _uri(config, consume=False):
    options={'overrideEndpoint':'true','uriEndpointOverride':config['endpoint'],'region':config['region'],'forcePathStyle':str(config['path_style_access']).lower(),'useDefaultCredentialsProvider':'true','autoCreateBucket':'false'}
    if consume:options.update(deleteAfterRead='false',moveAfterRead='false',includeBody='false',autocloseBody='true',maxMessagesPerPoll='10',delay='1000',initialDelay='1000',bridgeErrorHandler='true',prefix=config['prefix'])
    return 'aws2-s3://'+config['bucket']+'?'+urlencode(options)


def export_camel_k(document):
    discovered=discover_fleet(document)
    result={'report':discovered['report'],'profile':discovered['profile'],'files':{}}
    if not result['report']['ok']:return result
    lanes=result['profile']['lanes']
    if not 1<=len(lanes)<=3:
        result['report']['ok']=False;result['report']['errors'].append({'code':'camel_lane_bound','message':'This native Camel K profile currently supports one through three independent S3 lanes.'});return result
    routes=[]
    for lane in lanes:
        ident=hashlib.sha256(json.dumps(lane,sort_keys=True).encode()).hexdigest()[:16]
        routes.append('''        {
            BoundedMap identities = new BoundedMap();
            FileIdempotentRepository repository = new FileIdempotentRepository(new File(state, "%s.seen"), identities);
            repository.setMaxFileStoreSize(0);
            from(%s).routeId("lane-%s")
              .process(exchange -> {
                  Long length = exchange.getMessage().getHeader("CamelAwsS3ContentLength", Long.class);
                  if (length == null || length > 67108864L) throw new IllegalStateException("object_size_bound");
                  String key = exchange.getMessage().getHeader("CamelAwsS3Key", String.class);
                  String etag = exchange.getMessage().getHeader("CamelAwsS3ETag", String.class);
                  if (key == null || etag == null) throw new IllegalStateException("source_identity_missing");
                  String identity = %s + "\\n" + key + "\\n" + etag;
                  String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(identity.getBytes(StandardCharsets.UTF_8)));
                  if (identities.size() >= 10000 && !repository.contains(digest)) throw new IllegalStateException("durable_identity_capacity_reached");
                  exchange.setProperty("flowbridgeIdentity", digest);
              })
              .idempotentConsumer(exchangeProperty("flowbridgeIdentity"), repository).eager(false).removeOnFailure(true)
                .process(exchange -> {
                    Object key = exchange.getMessage().getHeader("CamelAwsS3Key");
                    Object type = exchange.getMessage().getHeader("CamelAwsS3ContentType");
                    Object length = exchange.getMessage().getHeader("CamelAwsS3ContentLength");
                    exchange.getMessage().removeHeaders("CamelAwsS3*");
                    exchange.getMessage().setHeader("CamelAwsS3Key", key);
                    exchange.getMessage().setHeader("CamelAwsS3ContentLength", length);
                    exchange.getMessage().setHeader("CamelAwsS3ContentType", type == null ? "application/octet-stream" : type);
                    exchange.getMessage().setHeader("CamelAwsS3Metadata", Map.of("media_type", %s));
                })
                .to(%s)
              .end();
        }
'''%(ident,json.dumps(_uri(lane['source'],True)),ident,json.dumps(lane['id']),json.dumps(lane['media_type']),json.dumps(_uri(lane['destination']))))
    java='''import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.processor.idempotent.FileIdempotentRepository;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.Map;

public class FlowbridgeS3 extends RouteBuilder {
    private FileChannel ownerChannel;
    private FileLock ownerLock;
    static final class BoundedMap extends HashMap<String,Object> {
        @Override public Object put(String key,Object value) {
            if (!containsKey(key) && size() >= 10000) throw new IllegalStateException("durable_identity_capacity_reached");
            return super.put(key,value);
        }
    }
    @Override public void configure() throws Exception {
        File state = new File(System.getenv().getOrDefault("FLOWBRIDGE_STATE_DIR", "/state"));
        if (!state.isDirectory() || !state.canWrite()) throw new IllegalStateException("persistent_state_required");
        ownerChannel = FileChannel.open(new File(state, ".owner.lock").toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        ownerLock = ownerChannel.tryLock();
        if (ownerLock == null) throw new IllegalStateException("persistent_state_already_owned");
        errorHandler(defaultErrorHandler().maximumRedeliveries(3).redeliveryDelay(1000));
'''+''.join(routes)+'    }\n}\n'
    integration={'apiVersion':'camel.apache.org/v1','kind':'Integration','metadata':{'name':'flowbridge-s3'},'spec':{'replicas':1,'sources':[{'name':'FlowbridgeS3.java','content':java}],'dependencies':['camel:aws2-s3'],'traits':{'camel':{'runtimeProvider':'plain-quarkus','runtimeVersion':'3.39.1'},'deployment':{'strategy':'Recreate'},'container':{'requestCPU':'100m','limitCPU':'1000m','requestMemory':'256Mi','limitMemory':'768Mi'},'mount':{'volumes':['flowbridge-camel-state:/state']},'environment':{'vars':['FLOWBRIDGE_STATE_DIR=/state']}}}}
    pvc={'apiVersion':'v1','kind':'PersistentVolumeClaim','metadata':{'name':'flowbridge-camel-state'},'spec':{'accessModes':['ReadWriteOnce'],'resources':{'requests':{'storage':'1Gi'}}}}
    result['report'].update(target='camel-k',runtime_verified=False,ready=False,artifact_kind='native_camel_k_integration',cutover_supported=False,runtime_requirement={'operator':'2.11.0','provider':'plain-quarkus','runtime_version':'3.39.1','observed_camel':'4.22.0'})
    result['report']['warnings'].append({'code':'native_execution_required','message':'Actual Camel K operator reconciliation, JVM route execution, persisted-state restart and trusted NiFi fencing must pass before production promotion. Retained source permits at-least-once backfill; native NiFi state is not imported.'})
    root=Path(__file__).resolve().parents[2]
    result['files']={'FlowbridgeS3.java':java,'camel-k-integration.json':json.dumps(integration,indent=2)+'\n','state-pvc.json':json.dumps(pvc,indent=2)+'\n','source-profile.json':json.dumps(result['profile'],indent=2)+'\n','LICENSE':(root/'LICENSE').read_text(),'README.md':'# Native Camel K S3 migration\n\nRequires Camel K 2.11.0 with the pinned plain-quarkus 3.39.1 runtime (observed Camel 4.22.0), a configured image registry, persistent writable PVC and scoped AWS credentials supplied through the deployment identity. Apply the PVC and Integration only in an isolated shadow environment until validation completes. Sources are retained. Each lane retains at most 10,000 durable identities, then fails closed instead of evicting deduplication history. Object size is bounded to 64 MiB. Keys, content types and literal media_type metadata are preserved; historical versions, custom encryption and ACL mappings are unsupported. One replica and one state volume owner only. Exactly-once is not claimed. NiFi stop/drain must be verified before the target can own production; the generated file cannot establish that fence.\n'}
    return result


def prepare_camel_k(document):
    """Prepare a native package; deployment and ownership remain separate gates."""
    return export_camel_k(document)


def inspect_camel_k(client, namespace, integration_name):
    """Read actual cluster resources using a trusted configured Kubernetes client.

    client.get(kind, namespace, name) returns decoded API resource documents.
    Resource readiness is not checksum equivalence or a source-ownership fence.
    """
    import re
    if not all(isinstance(value,str) and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}',value) for value in (namespace,integration_name)):
        return {'ok':False,'error':'invalid_kubernetes_identity','cutover_supported':False}
    try:
        integration=client.get('Integration',namespace,integration_name)
        deployment=client.get('Deployment',namespace,integration_name)
        operator=client.get('Deployment',namespace,'camel-k-operator')
        uid=integration.get('metadata',{}).get('uid')
        owned=bool(uid) and any(ref.get('uid')==uid and ref.get('kind')=='Integration' for ref in deployment.get('metadata',{}).get('ownerReferences',[]))
        current=deployment.get('status',{}).get('observedGeneration')==deployment.get('metadata',{}).get('generation')
        ready=deployment.get('status',{}).get('readyReplicas',0)
        expected=deployment.get('spec',{}).get('replicas',1)
        running=integration.get('status',{}).get('phase')=='Running'
        operator_ready=operator.get('status',{}).get('readyReplicas',0)>0
        return {'ok':True,'integration_phase':integration.get('status',{}).get('phase'),'runtime_observed':owned and current and ready==expected==1 and running and operator_ready,'operator_ready':operator_ready,'ready_replicas':ready,'owned_deployment':owned,'cutover_supported':False,'data_equivalence_verified':False,'reason':'Source fencing and checksum reconciliation are separate required gates.'}
    except Exception:
        return {'ok':False,'error':'kubernetes_inspection_failed','cutover_supported':False}
