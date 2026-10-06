# Kubernetes and OpenShift deployment

The Helm chart in `charts/flowbridge` deploys the stateless Flowbridge workbench. It does not deploy NiFi, SeaTunnel, Camel K, Kafka brokers or generated applications. File conversion remains subject to the [support matrix](support-matrix.md).

The application has **no built-in user authentication**. The default deployment uses a ClusterIP Service and local port forwarding. ClusterIP limits network exposure; it does not authenticate other workloads in the cluster. Public or shared exposure requires your organization's authenticated gateway/proxy and network controls. Host/origin checks defend browser requests and are not a login system.

## Prerequisites and image

Use a currently supported Helm client, a Kubernetes or OpenShift cluster, and a namespace in which you can create the chart's namespaced resources. The chart uses Helm chart API v2 and declares Kubernetes `>=1.25.0-0`; this is a manifest compatibility floor, not a recommendation to run an end-of-life Kubernetes release or evidence of testing every newer version. Choose a currently maintained cluster release consistent with your organization's support policy. Use an image that the cluster can pull and whose CPU architecture matches its nodes. The checked-in chart is available from this repository; an OCI chart URL is not evidence that a package has been published.

To build and push to a registry you control, replace the example registry/name below:

```sh
FLOWBRIDGE_IMAGE=registry.example.com/your-team/nifi-flowbridge
FLOWBRIDGE_TAG=0.1.0
# Match the cluster architecture; use a multi-platform build for mixed nodes.
docker build -t "$FLOWBRIDGE_IMAGE:$FLOWBRIDGE_TAG" .
docker push "$FLOWBRIDGE_IMAGE:$FLOWBRIDGE_TAG"
```

An image built on an ARM laptop is not automatically an AMD64 image. For both architectures, use an appropriately configured builder with `docker buildx build --platform linux/amd64,linux/arm64 --push`. Authenticate to your registry through its normal mechanism; do not put registry passwords in chart values or Git. Configure `imagePullSecrets` if the cluster needs an existing pull secret.

If a release publishes a public GHCR image, use the exact repository/tag or digest recorded in that release's verified installation instructions. Until then, build/push your own image as above. Publication of source code, a chart archive and a container image are separate operations.

## Install the local chart

Run from the repository root, after setting the image variables above:

```sh
helm lint charts/flowbridge
helm upgrade --install flowbridge ./charts/flowbridge \
  --namespace flowbridge --create-namespace \
  --set image.repository="$FLOWBRIDGE_IMAGE" \
  --set image.tag="$FLOWBRIDGE_TAG" \
  --wait --timeout 3m
kubectl -n flowbridge rollout status deployment/flowbridge-flowbridge
kubectl -n flowbridge port-forward service/flowbridge-flowbridge 8790:8790
```

Open `http://localhost:8790` while the forwarding process is running. Keep forwarding bound to localhost. On OpenShift, `oc` can replace `kubectl`; use an existing project if namespace/project creation is managed by your administrator. The Service keeps a stable internal endpoint, and port forwarding establishes an operator-controlled local connection. [Kubernetes Services](https://kubernetes.io/docs/concepts/services-networking/service/), [port forwarding](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_port-forward/).

To inspect a failed rollout:

```sh
kubectl -n flowbridge get pods
kubectl -n flowbridge describe deployment flowbridge-flowbridge
kubectl -n flowbridge get events --sort-by=.lastTimestamp
kubectl -n flowbridge logs deployment/flowbridge-flowbridge
```

Check image pull access, architecture, resource quotas and admission-policy messages before changing security settings. `ImagePullBackOff` usually needs an accessible image or pull credentials, not an elevated container. An uploaded flow is processed in memory; the chart requires no persistent volume or production-platform credentials.

## OpenShift security posture

The chart is designed to run under restricted security constraints: non-root, no privilege escalation, all Linux capabilities dropped, RuntimeDefault seccomp, and a read-only root filesystem. It does not pin `runAsUser` or `fsGroup`; OpenShift can assign namespace-appropriate IDs. The container must be able to read its application files under that arbitrary non-root UID without writing into its source directory. No `anyuid`, privileged SCC, host filesystem mount or cluster-administrator grant is required by the application design. [Red Hat image guidelines](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/html/images/creating-images).

These controls align with `restricted-v2`. Actual admission depends on cluster policy and version; newer documentation also describes `restricted-v3` and user-namespace constraints. Do not weaken SCCs to work around an uninvestigated failure. After a real OpenShift install, inspect the pod's `openshift.io/scc` annotation and effective security context to establish what admitted it. [OpenShift security context constraints](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/observability/authentication_and_authorization/index).

## Optional Ingress or OpenShift Route

Keep both disabled for local port-forwarding use. Choose one exposure mechanism when deploying behind an authenticated organizational access layer. Kubernetes Ingress requires an installed ingress controller; OpenShift Route requires the `route.openshift.io/v1` API and a working router. A Route or TLS certificate alone does not add authentication.

Set `ingress.host` or `route.host` to the exact external hostname. The chart derives the allowed hosts and HTTPS origins from that value and configures `FLOWBRIDGE_ALLOWED_HOSTS` and `FLOWBRIDGE_ALLOWED_ORIGINS`. Preserve the Host and Origin expected by the application through your proxy. Do not use wildcard origin allowances. Follow the chart's `values.yaml` for the complete, current keys and validation rules. Do not enable both Ingress and Route for the same release.

An edge-terminated OpenShift Route encrypts the client-to-router leg; it forwards HTTP to this application's Service. The application does not serve TLS itself, so passthrough TLS and re-encryption to the current HTTP container are not appropriate without an additional TLS-serving proxy. Protect the internal network according to organizational policy. [OpenShift Route termination](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/html/ingress_and_load_balancing/routes).

## Render and validate without a cluster

```sh
helm lint charts/flowbridge
helm template flowbridge charts/flowbridge --namespace flowbridge > /tmp/flowbridge-kubernetes.yaml
```

For an offline OpenShift Route render, create a values file containing the following settings, replacing the example hostname. Only install this exposure after organizational authentication/access controls are in place:

```yaml
# your-reviewed-openshift-values.yaml
route:
  enabled: true
  host: flowbridge.apps.example.com
```

The router certificate must cover that hostname. For Kubernetes Ingress instead, set `ingress.enabled: true`, `ingress.host`, `ingress.tlsSecretName` and, if needed, `ingress.className` and controller-specific annotations for your authenticated access layer. The chart does not install that layer. Explicitly advertise the Route API for local rendering:

```sh
helm template flowbridge charts/flowbridge \
  --namespace flowbridge \
  --api-versions route.openshift.io/v1 \
  -f your-reviewed-openshift-values.yaml > /tmp/flowbridge-openshift.yaml
```

Helm's local rendering does not check whether a live cluster supports the APIs or whether admission controllers will accept the result. Supplying `--api-versions` simulates capability discovery; it does not install Route support on Kubernetes. [Helm template reference](https://helm.sh/docs/helm/helm_template/).

Where a cluster is available, add a server-side dry run of the reviewed render, then perform an install and functional test. Successful linting, render tests, image build, arbitrary-UID container smoke tests and live-cluster verification are distinct evidence. Do not describe Kubernetes/OpenShift deployment as verified unless a real cluster has accepted and run the workload.

## OCI Helm distribution

Maintainers can package and publish the chart to an OCI registry. Helm infers the final chart name and version from `Chart.yaml`; the push destination is the registry namespace, not a manually appended filename. [Helm OCI registry guide](https://helm.sh/docs/topics/registries/).

```sh
helm package charts/flowbridge --destination /tmp
# After registry authentication and authorization:
helm push /tmp/flowbridge-0.1.0.tgz oci://ghcr.io/davano-innovation-lab/charts
```

Only after that version is published and pull-tested should users install it with:

```sh
helm show chart oci://ghcr.io/davano-innovation-lab/charts/flowbridge --version 0.1.0
helm upgrade --install flowbridge \
  oci://ghcr.io/davano-innovation-lab/charts/flowbridge --version 0.1.0 \
  --namespace flowbridge --create-namespace \
  --set image.repository="$FLOWBRIDGE_IMAGE" \
  --set image.tag="$FLOWBRIDGE_TAG" --wait
```

These are publication and installation instructions, not a claim that the OCI package is already public. Use the repository-local chart if no verified published version is listed in the release.

The default resources request 64 MiB and 50 millicores and limit the application to 256 MiB and one CPU. These are application pod values, not the memory allocation for generated Kafka/NiFi/SeaTunnel/Camel applications. Adjust using measured workload evidence.

## Remove the workbench

```sh
helm uninstall flowbridge --namespace flowbridge
```

Resource names use `<release>-flowbridge` by default; the examples install release `flowbridge`, producing `flowbridge-flowbridge`. A `fullnameOverride` changes those names.

This removes the Helm-managed application resources. It does not delete the namespace, your image registry, generated downloads, or any independently deployed data platform.
