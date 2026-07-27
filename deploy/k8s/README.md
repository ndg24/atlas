# Atlas on Kubernetes

The production equivalent of `deploy/docker-compose.yml`: the same services
(Postgres+pgvector, MinIO, Redis, Ollama, catalog, 3 workers, coordinator,
ai-service, dashboard, Jaeger, Prometheus), wired with the same environment
contract, deployed as a `kustomize` base so `coordinator`/`dashboard` scale
independently from the single-instance `worker` fleet.

## Prerequisites

- A cluster (`kind`/`k3d`/`minikube` for local testing, or a real cluster).
- A **ReadWriteMany**-capable `StorageClass` (NFS, AWS EFS, GCP Filestore,
  Azure Files, Longhorn, ...) for `data-pvc.yaml`. This is the one manifest
  a default `ReadWriteOnce` cloud StorageClass (e.g. GCP/AWS's default)
  cannot satisfy — see the comment in `data-pvc.yaml` for why the worker
  fleet specifically needs shared, multi-attach storage.
- (Optional) an ingress controller (e.g. `ingress-nginx`) if you want
  `ingress.yaml`'s external dashboard route; otherwise use
  `kubectl port-forward` (below) and delete/ignore that file.

## Build and load images

There's no registry pushed to by default — `kustomization.yaml` points at
`atlas/{coordinator,worker,ai-service,dashboard}:latest` with
`imagePullPolicy: IfNotPresent`, so a local cluster that already has these
images loaded needs no further changes:

```
# from the repo root
docker build -f coordinator/Dockerfile -t atlas/coordinator:latest .
docker build -f engine/Dockerfile      -t atlas/worker:latest .
docker build -f ai-service/Dockerfile  -t atlas/ai-service:latest .
docker build -f dashboard/Dockerfile   -t atlas/dashboard:latest .

# kind:
kind load docker-image atlas/coordinator:latest atlas/worker:latest atlas/ai-service:latest atlas/dashboard:latest

# minikube:
minikube image load atlas/coordinator:latest
minikube image load atlas/worker:latest
minikube image load atlas/ai-service:latest
minikube image load atlas/dashboard:latest
```

For a real cluster, tag and push to your registry, then repoint
`kustomization.yaml`'s `images:` section (`newName`) at it — one place,
not four.

## Deploy

```
kubectl apply -k deploy/k8s
kubectl -n atlas get pods -w
```

`secret.yaml` ships the same dev-insecure defaults
`deploy/docker-compose.yml` hardcodes (`JWT_SECRET=dev-insecure-secret-change-me`,
`atlas`/`atlas` Postgres and MinIO credentials) so the stack comes up
out of the box — **override every value in it before deploying anywhere
real**, e.g.:

```
kubectl create secret generic atlas-secrets -n atlas \
  --from-literal=JWT_SECRET="$(openssl rand -hex 32)" \
  --from-literal=POSTGRES_USER=atlas \
  --from-literal=POSTGRES_PASSWORD="$(openssl rand -hex 16)" \
  --from-literal=POSTGRES_DB=atlas \
  --from-literal=MINIO_ROOT_USER=atlas \
  --from-literal=MINIO_ROOT_PASSWORD="$(openssl rand -hex 16)" \
  --dry-run=client -o yaml | kubectl apply -f -
```
(and update `DATABASE_URL` in `configmap.yaml` to match, same as
docker-compose's own convention of a plain connection string rather than
one assembled from parts).

## Access it

```
kubectl -n atlas port-forward svc/dashboard 3000:3000    # http://localhost:3000
kubectl -n atlas port-forward svc/coordinator 8080:8080  # REST API / atlas-cli / atlas-sdk
kubectl -n atlas port-forward svc/prometheus 9090:9090
kubectl -n atlas port-forward svc/jaeger 16686:16686      # trace UI
```

Or, with an ingress controller installed, `ingress.yaml` routes
`atlas.local` (edit the `host:` to your own) to the dashboard.

## Pull an Ollama model

Same as docker-compose — the ai-service's default provider is `ollama`,
which needs a model actually pulled once:

```
kubectl -n atlas exec deploy/ollama -- ollama pull llama3
kubectl -n atlas exec deploy/ollama -- ollama pull nomic-embed-text   # Phase 8 literature embeddings
```

## Ingesting data into the cluster

`atlas-cli ingest` (`engine/crates/atlas-cli`) writes `.atlas`/Parquet files
to a local filesystem path (`--data-dir`) and commits the resulting
manifest to the catalog over gRPC — there's no S3/MinIO endpoint flag wired
up yet (`atlas-storage`'s `object_store` abstraction supports it; the CLI
doesn't expose it). That means the CLI needs filesystem access to the same
volume the workers read from (`atlas-data`, see `data-pvc.yaml`), same
constraint docker-compose has via its `./data` bind mount. Run it from a
pod that mounts that PVC, e.g.:

```
kubectl run atlas-cli -n atlas --rm -it --image atlas/worker:latest \
  --overrides='{"spec":{"containers":[{"name":"atlas-cli","image":"atlas/worker:latest","command":["sleep","infinity"],"volumeMounts":[{"name":"data","mountPath":"/data"}]}],"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"atlas-data"}}]}}' \
  -- sleep infinity
# (atlas-worker's image doesn't ship atlas-cli's binary today -- build one
# that does, or `kubectl cp` a locally-built atlas-cli binary into a pod
# that already mounts atlas-data, then exec it from there.)
```

then, from inside that pod/exec session:

```
atlas-cli ingest --file patients.csv --dataset patients \
  --data-dir /data --catalog-addr http://catalog:9091
```

After that, `POST /query` (or `atlas-sdk`/`atlas-cli query --dataset` from
outside the cluster via the port-forwarded coordinator) reads the same
files through the workers automatically — no separate registration step.

## What's intentionally different from docker-compose

- **`worker` is a `StatefulSet`**, not three copy-pasted `Deployment`s —
  `coordinator`'s `WORKER_ADDRS` needs 3 distinct, individually
  heartbeat-tracked addresses (`coordinator/internal/scheduler.Registry`);
  the headless `worker` Service gives each pod a stable DNS name
  (`worker-0.worker`, `worker-1.worker`, `worker-2.worker`) instead.
- **`catalog` and `coordinator` share one image** (`atlas/coordinator`),
  same as `coordinator/Dockerfile` building both binaries — `command:`
  picks which one runs, mirroring docker-compose's `entrypoint:` override.
- **`coordinator` and `dashboard` run 2 replicas** — both are stateless
  (state lives in Postgres/Redis), unlike `worker`.
