# Flowbridge Helm chart

This application chart packages the stateless Flowbridge importer/exporter UI for Kubernetes and OpenShift. It uses the Helm v2 chart format, supported by Helm 3 and later. The chart declares Kubernetes 1.25 or later. OpenShift must provide the `route.openshift.io/v1` API when Route exposure is enabled.

The default deployment is a private ClusterIP service on port 8790. It creates no persistent volumes, host mounts, cluster roles, or external target deployments. Uploaded flows are processed in memory. Exported projects still need review and target-runtime validation.

## Install from this repository

```sh
helm lint charts/flowbridge --strict
helm upgrade --install flowbridge charts/flowbridge \
  --namespace flowbridge --create-namespace --wait
kubectl --namespace flowbridge port-forward service/flowbridge-flowbridge 8790:8790
```

Open `http://localhost:8790`, then run `helm test flowbridge --namespace flowbridge` to check the in-cluster service connection. The test uses the application image, without downloading an additional diagnostic image.

The default image reference is `ghcr.io/davano-innovation-lab/nifi-flowbridge:0.1.0`. A chart file does not establish that an image has been published. Before installation, verify that this tag is available from your cluster, or build and push the source to a registry you control:

```sh
docker build -t YOUR_REGISTRY/nifi-flowbridge:0.1.0 .
docker push YOUR_REGISTRY/nifi-flowbridge:0.1.0
helm upgrade --install flowbridge charts/flowbridge \
  --namespace flowbridge --create-namespace \
  --set image.repository=YOUR_REGISTRY/nifi-flowbridge \
  --set-string image.tag=0.1.0 --wait
```

Use a lowercase registry/repository name in place of `YOUR_REGISTRY`. For a private registry, set `imagePullSecrets` to existing Kubernetes secret names; never put registry credentials in chart values. For reproducible deployment, set `image.digest=sha256:...`, which takes precedence over the tag.

## Kubernetes Ingress

Provision a TLS secret and configure your ingress controller's authentication gateway before exposing the application outside a trusted network. Flowbridge has no built-in user authentication; Host and Origin checks are not authentication.

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

No privileged SCC or `anyuid` grant is needed by the chart's security design. Both application and Helm test Pods omit `runAsUser`, `runAsGroup`, and `fsGroup`, allowing OpenShift admission to assign a namespace UID. Containers require non-root execution, drop all Linux capabilities, disallow privilege escalation, use `RuntimeDefault` seccomp, and have a read-only root filesystem. Service account token mounting is disabled on both the service account and Pods. Custom images must support non-root and arbitrary UIDs and must not require writable image directories.

## Values

| Value | Default | Purpose |
| --- | --- | --- |
| `replicaCount` | `1` | Stateless application replicas, 1–20. |
| `nameOverride`, `fullnameOverride` | empty | Override generated resource names. |
| `image.repository` | `ghcr.io/davano-innovation-lab/nifi-flowbridge` | Application repository. |
| `image.tag` | `0.1.0` | Application image tag. |
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
| `tests.enabled` | `true` | Render a Helm test Pod that calls the service health endpoint. |

Security settings are intentionally not configurable through generic Pod/container overrides. Resource quantities are strings; quote a CPU limit such as `"1"` in values files.

## Validation and limitations

`values.schema.json` validates names, image settings, resource quantities, required exposure settings and mutually exclusive Ingress/Route selection. Liveness, readiness and startup probes run Python against `127.0.0.1:8790/api/health`; they avoid Kubernetes HTTP probe Host headers being rejected by the application's host validation. The chart sets comma-separated `FLOWBRIDGE_ALLOWED_HOSTS` and `FLOWBRIDGE_ALLOWED_ORIGINS`; it does not enable wildcard origins or trust arbitrary forwarded headers.

Rendering and linting are offline checks, not evidence of an installation on Kubernetes or OpenShift. Successful real-cluster admission, image pulling, ingress/controller integration, SCC assignment, and network connectivity must be verified on the deployment cluster. The Helm test checks service health only, not exported migration correctness.

References: [Helm chart format](https://helm.sh/docs/topics/charts/), [Kubernetes restricted Pod Security Standard](https://kubernetes.io/docs/concepts/security/pod-security-standards/), and [OpenShift image guidance](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/html/images/creating-images).
