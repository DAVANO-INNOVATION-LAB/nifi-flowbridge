# Flowbridge Helm chart

This application chart packages the Flowbridge importer/exporter UI and an optional connected Kafka migration service for Kubernetes and OpenShift. It uses the Helm v2 chart format, supported by Helm 3 and later. The chart declares Kubernetes 1.25 or later. OpenShift must provide the `route.openshift.io/v1` API when Route exposure is enabled.

The default deployment is a private ClusterIP service on port 8790. It creates no persistent volumes, host mounts, cluster roles, or external target deployments. Uploaded flows are processed in memory. Exported projects still need review and target-runtime validation.

## Install from this repository

```sh
helm lint charts/flowbridge --strict
helm upgrade --install flowbridge charts/flowbridge \
  --namespace flowbridge --create-namespace --wait
kubectl --namespace flowbridge port-forward service/flowbridge-flowbridge 8790:8790
```

Open `http://localhost:8790`, then run `helm test flowbridge --namespace flowbridge` to check the in-cluster service connection. The test uses the application image, without downloading an additional diagnostic image.

The default image reference is `ghcr.io/davano-innovation-lab/nifi-flowbridge:0.3.0`. A chart file does not establish that an image has been published. Before installation, verify that this tag is available from your cluster, or build and push the source to a registry you control:

```sh
docker build -t YOUR_REGISTRY/nifi-flowbridge:0.3.0 .
docker push YOUR_REGISTRY/nifi-flowbridge:0.3.0
helm upgrade --install flowbridge charts/flowbridge \
  --namespace flowbridge --create-namespace \
  --set image.repository=YOUR_REGISTRY/nifi-flowbridge \
  --set-string image.tag=0.3.0 --wait
```

Use a lowercase registry/repository name in place of `YOUR_REGISTRY`. For a private registry, set `imagePullSecrets` to existing Kubernetes secret names; never put registry credentials in chart values. For reproducible deployment, set `image.digest=sha256:...`, which takes precedence over the tag.

## Kubernetes Ingress

Provision a TLS secret and configure your ingress controller's authentication gateway before exposing the application outside a trusted network. The static UI and file-conversion endpoints have no built-in user login. Connected migration operations require the owner bearer token when enabled; Host and Origin checks are not authentication.

```yaml
ingress:
  enabled: true
  className: nginx
  host: flowbridge.example.com
  tlsSecretName: flowbridge-tls
  annotations: {}
```

Save these values in a file and pass `-f your-values.yaml` to `helm upgrade --install`. The host and TLS secret are required. HTTP-to-HTTPS redirect behavior depends on the ingress controller; configure it according to that controller's documentation. The application allowlist contains the exact configured host and its HTTPS origin. Host rewrites by an upstream proxy must be configured consistently with this allowlist.

## OpenShift Route

```yaml
route:
  enabled: true
  host: flowbridge.apps.example.com
  annotations: {}
```

The Route uses edge TLS termination, the router's certificate, and redirects insecure connections to HTTPS. Choose a host covered by the router's certificate and configure DNS. A host is required so that the application's Host and Origin allowlists can be set before the Pod starts. Ingress and Route cannot both be enabled.

No privileged SCC or `anyuid` grant is needed by the chart's security design. By default, application and Helm test Pods omit `runAsUser`, `runAsGroup`, and `fsGroup`, allowing OpenShift admission to assign a namespace UID and group. Containers require non-root execution, drop all Linux capabilities, disallow privilege escalation, use `RuntimeDefault` seccomp, and have a read-only root filesystem. Service account token mounting is disabled on both the service account and Pods. Custom images must support non-root and arbitrary UIDs and must not require writable image directories.

## Values

| Value | Default | Purpose |
| --- | --- | --- |
| `replicaCount` | `1` | Stateless replicas, 1–20; live migration requires exactly 1. |
| `nameOverride`, `fullnameOverride` | empty | Override generated resource names. |
| `image.repository` | `ghcr.io/davano-innovation-lab/nifi-flowbridge` | Application repository. |
| `image.tag` | `0.3.0` | Application image tag. |
| `image.digest` | empty | Optional SHA-256 digest, overrides tag. |
| `image.pullPolicy` | `IfNotPresent` | Kubernetes image pull policy. |
| `imagePullSecrets` | `[]` | Existing registry pull secrets as `{name: secret-name}` entries. |
| `service.port` | `8790` | Fixed service and application port. |
| `clusterDomain` | `cluster.local` | Cluster DNS domain for the service Host allowlist. |
| `resources.requests` | `64Mi`, `50m` | Requested memory and CPU. |
| `resources.limits` | `256Mi`, `1` | Memory and CPU limits. |
| `ingress.enabled` | `false` | Enable Kubernetes Ingress. |
| `ingress.host`, `ingress.tlsSecretName` | empty | Required when Ingress is enabled. |
| `ingress.className`, `ingress.annotations` | empty, `{}` | Ingress-controller settings. |
| `route.enabled` | `false` | Enable OpenShift Route. |
| `route.host`, `route.annotations` | empty, `{}` | Required explicit host and router settings. |
| `live.enabled` | `false` | Enable authenticated, connected Kafka migration. |
| `live.tokenSecretName` | empty | Existing Secret containing the `token` key; required for live mode. |
| `live.storage.existingClaim` | empty | Existing writable PVC for job state; required for live mode. |
| `podSecurityContext.fsGroup` | unset | Optional storage-access group for Kubernetes; omit for OpenShift admission. |
| `tests.enabled` | `true` | Render a Helm test Pod that calls the service health endpoint. |

Security settings are intentionally not configurable through generic Pod/container overrides. Resource quantities are strings; quote a CPU limit such as `"1"` in values files.

## Connected Kafka migration

Live mode is disabled by default. When enabled, the chart requires an existing Secret and a persistent volume claim in the release namespace. It enforces one replica and the `Recreate` deployment strategy because the service owns local migration workers and SQLite state. An upgrade interrupts transfers; it does not resume them automatically. Job state survives through the PVC, while broker credentials must be supplied again after restart. Records already copied are not removed by cancellation or interruption.

Create the namespace and a PVC using a storage class supported by your cluster. This example requests the cluster's default class; set `storageClassName` explicitly if your administrator requires one:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: flowbridge-data
  namespace: flowbridge
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```

Create an owner-token Secret from a protected local file containing a strong random token, without putting the token in Helm values or shell command arguments:

```sh
kubectl --namespace flowbridge create secret generic flowbridge-live-owner \
  --from-file=token=/secure/path/owner-token
```

Use these Helm values after the PVC is bound and the Secret exists:

```yaml
live:
  enabled: true
  tokenSecretName: flowbridge-live-owner
  storage:
    existingClaim: flowbridge-data
replicaCount: 1
```

The token is mounted read-only at `/run/secrets/flowbridge-live-token`; it is never rendered into environment variables or chart output. The application receives `FLOWBRIDGE_LIVE_ENABLED=true`, `FLOWBRIDGE_LIVE_TOKEN_FILE=/run/secrets/flowbridge-live-token`, and `FLOWBRIDGE_DATA_DIR=/data`. The Secret volume uses readable file permissions so an assigned arbitrary UID can read it; no other Pod receives this mount. Only the application container mounts the PVC at `/data`, with its root filesystem still read-only.

The storage driver must support writable access by the Pod's assigned group. On Kubernetes, if the provisioned volume needs an explicit access group, set `podSecurityContext.fsGroup` to a group approved by your administrator, for example:

```yaml
podSecurityContext:
  fsGroup: 65532
```

On OpenShift, leave that setting absent so namespace admission can assign an allowed group. Do not grant `anyuid` or add a privileged ownership-changing init container. Verify the storage class's group-permission behavior if startup reports that `/data` is not writable. Some storage drivers or pre-existing volumes require administrator-side ownership configuration; the chart does not change ownership itself.

The default remains a private ClusterIP service. Use port-forwarding on a trusted workstation, or configure the existing TLS Ingress/Route settings and an appropriate authentication gateway. The browser page itself is public to clients that can reach it; connected actions require the owner token. Enter that token in the connected-migration workspace. Start, cancellation, and cutover do not happen automatically. Operators must stop their external producers and consumers before authorizing cutover.

## Validation and limitations

`values.schema.json` validates names, image settings, resource quantities, required exposure settings and mutually exclusive Ingress/Route selection. Liveness, readiness and startup probes run Python against `127.0.0.1:8790/api/health`; they avoid Kubernetes HTTP probe Host headers being rejected by the application's host validation. The chart sets comma-separated `FLOWBRIDGE_ALLOWED_HOSTS` and `FLOWBRIDGE_ALLOWED_ORIGINS`; it does not enable wildcard origins or trust arbitrary forwarded headers.

Rendering and linting are offline checks, not evidence of an installation on Kubernetes or OpenShift. Successful real-cluster admission, image pulling, ingress/controller integration, SCC assignment, and network connectivity must be verified on the deployment cluster. The Helm test checks service health only, not exported migration correctness.

References: [Helm chart format](https://helm.sh/docs/topics/charts/), [Kubernetes restricted Pod Security Standard](https://kubernetes.io/docs/concepts/security/pod-security-standards/), and [OpenShift image guidance](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/html/images/creating-images).
