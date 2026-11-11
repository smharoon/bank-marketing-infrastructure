# Bank Marketing on Kubernetes

For the complete reset/start procedure, dashboard commands, and end-to-end verification, see [RESET_AND_START.md](RESET_AND_START.md). Run `python3 kubernetes/check_stack.py` to process real data through all three deployed flows, or add `--verify-only` to inspect existing results.

These manifests run the existing `bank_dataops/` and `bank_mlops/` applications alongside PostgreSQL, Prefect, and MLflow. Run the commands below from the repository root. DataOps and MLOps now share their local folders with Minikube; their previous application PVCs are retained as historical backups. Avoid running Compose application services against these same folders concurrently.

## Requirements

- A Kubernetes cluster with a default StorageClass that dynamically provisions `ReadWriteOnce` volumes, or pre-provisioned volumes matching the six claims in `02-storage.yaml`.
- `kubectl` configured for that cluster, Docker for building the application images, and OpenSSL for generating the database password.
- The local Minikube folder mount services from `bank-dataops-mount.service` and `bank-mlops-mount.service`, configured as described in [RESET_AND_START.md](RESET_AND_START.md). For another cluster, replace the application hostPath volumes with suitable shared storage.
- For a local demo, Minikube with roughly 4 CPUs, 8 GiB memory, and enough disk space for the images and up to 42 GiB of persistent storage. CPU/memory limits are ceilings; requests determine scheduling.

## 1. Build application images

The manifests use `bank-dataops:1.0.2` and `bank-mlops:1.0.2`. Both MLOps deployments use the same image, with different commands.

```bash
docker build -t bank-dataops:1.0.2 ./bank_dataops
docker build -t bank-mlops:1.0.2 ./bank_mlops
```

For Minikube, start the cluster if needed and load the images:

```bash
minikube start --cpus=4 --memory=8192 --disk-size=60g
minikube image load --daemon --overwrite bank-dataops:1.0.2
minikube image load --daemon --overwrite bank-mlops:1.0.2
```

For a remote cluster, tag and push both images to a registry accessible to the cluster, then replace **all** application image references in `06-dataops.yaml`, `07-mlops-train.yaml`, and `08-mlops-evaluate.yaml`, including their init containers. Configure `imagePullSecrets` if the registry requires authentication. Infrastructure images match the existing Compose stack; pin Prefect's `3-latest` tag to a tested immutable version or digest for repeatable deployments.

## 2. Create configuration and credentials

Review `01-config.yaml` before first startup. PostgreSQL defaults to user `bank` and database `bank_data`. No credentials are committed in these manifests. Create the Secret once; the generated hexadecimal password is safe to embed in a database URL without additional URL encoding.

```bash
kubectl apply -f kubernetes/00-namespace.yaml
kubectl apply -f kubernetes/01-config.yaml
BANK_DB_PASSWORD=$(openssl rand -hex 24)
kubectl -n bank-marketing create secret generic bank-secrets \
  --from-literal=POSTGRES_PASSWORD="$BANK_DB_PASSWORD" \
  --from-literal=DATABASE_URL="postgresql+psycopg2://bank:${BANK_DB_PASSWORD}@bank-postgres:5432/bank_data"
unset BANK_DB_PASSWORD
```

Do not regenerate the Secret on every deployment: PostgreSQL uses its initial password from the persistent database. Subsequent password rotation requires changing the PostgreSQL role and both Secret values together. If changing the user or database in the ConfigMap, update the URL to match. For passwords containing URL-reserved characters, percent-encode the password component of `DATABASE_URL`.

## 3. Deploy and check readiness

Start the local folder mounts before deploying the applications:

```bash
systemctl --user enable --now "$PWD/kubernetes/bank-dataops-mount.service"
systemctl --user enable --now "$PWD/kubernetes/bank-mlops-mount.service"
minikube -p minikube ssh -- 'findmnt /mnt/bank-dataops'
minikube -p minikube ssh -- 'findmnt /mnt/bank-mlops'
```

Wait until both mounts are ready, then:

```bash
kubectl apply -f kubernetes/
kubectl -n bank-marketing rollout status deployment/bank-postgres --timeout=300s
kubectl -n bank-marketing rollout status deployment/bank-prefect --timeout=900s
kubectl -n bank-marketing rollout status deployment/bank-mlflow --timeout=600s
kubectl -n bank-marketing rollout status deployment/bank-dataops --timeout=600s
kubectl -n bank-marketing rollout status deployment/bank-mlops-train --timeout=600s
kubectl -n bank-marketing rollout status deployment/bank-mlops-evaluate --timeout=600s
kubectl -n bank-marketing get pods,svc,pvc
```

The application init containers check the database with `SELECT 1` and check the Prefect health endpoint. MLOps also checks MLflow. A second init container creates the input/archive/output directories before the flow server starts. PostgreSQL, Prefect, and MLflow have startup, readiness, and liveness probes. The application processes use Kubernetes process supervision; application rollout completion does not prove Prefect deployment registration or a successful flow run. Confirm registration in the UI or application logs:

```bash
kubectl -n bank-marketing logs deployment/bank-dataops --tail=50
kubectl -n bank-marketing logs deployment/bank-mlops-train --tail=50
kubectl -n bank-marketing logs deployment/bank-mlops-evaluate --tail=50
```

These remain long-lived Prefect `flow.serve()` processes: DataOps and evaluation run every 120 seconds, and training is manual. No separate worker, Kubernetes Job, or CronJob is needed for the existing application design.

## 4. Open dashboards

Run each command in a separate terminal:

```bash
kubectl -n bank-marketing port-forward service/bank-prefect 4200:4200
```

```bash
kubectl -n bank-marketing port-forward service/bank-mlflow 5000:5000
```

Open Prefect at <http://localhost:4200> and MLflow at <http://localhost:5000>. Services use ClusterIP. The Prefect browser API URL defaults to `http://localhost:4200/api`; if you expose the UI through a different address, update `PREFECT_SERVER_UI_API_URL` in the ConfigMap and restart `bank-prefect`. MLflow's allowed hosts match its internal service names and the documented local port forward.

## 5. Supply CSVs and run flows

DataOps scans the local `bank_dataops/data/incoming` folder automatically every 120 seconds. Place a complete CSV there; reports appear directly in local `bank_dataops/reports` and processed files move to local `bank_dataops/data/archive`. No terminal trigger or upload helper is required. Completed checksums are deduplicated; missing reports for those batches are regenerated.

For example, place the one-row DataOps plumbing example in the local folder (it is too small for model training):

```bash
cp bank_dataops/examples/example_bank_marketing.csv bank_dataops/data/incoming/
```

Wait for the next DataOps scheduled run.

For training, place labeled CSVs in local `bank_mlops/train_model/incoming`, then select `bank-marketing-train-model/manual-training` in Prefect and click **Run**. Training remains ad hoc with no schedule. Each run combines new pending files, trains the candidate models, and registers the champion in MLflow. Successful CSVs move directly to local `bank_mlops/train_model/processed`. Only new file checksums are trained; completed files are deduplicated.

For evaluation, put a labeled CSV into local `bank_mlops/evaluate_model/incoming` and wait for the next two-minute schedule. It uses the saved MLflow champion, writes prediction CSVs directly into local `bank_mlops/outputs`, and moves the input to local `bank_mlops/evaluate_model/processed`. Use observations not used in training for meaningful performance measurements. An input arriving before the first successful training run remains available for retry. Failed MLOps inputs stay in incoming for investigation/retry.

No per-file upload or download command is required for either MLOps folder. Training is triggered through Prefect; evaluation is automatic. Both flow servers execute at most one run at a time. The old application PVCs still contain historical files; PostgreSQL, Prefect, and MLflow continue using their persistent volumes.

## Storage and deployment behavior

| Claim | Size | Mount / contents |
| --- | --- | --- |
| `postgres-data` | 10 GiB | PostgreSQL database at `/var/lib/postgresql/data/pgdata` |
| `prefect-data` | 2 GiB | Prefect SQLite database and state under `/root/.prefect` |
| `mlflow-data` | 10 GiB | SQLite metadata and server-proxied model artifacts under `/mlflow-data` |
| `dataops-data` | 5 GiB | Retained historical PVC; DataOps now uses local `bank_dataops/data` |
| `dataops-reports` | 5 GiB | Retained historical PVC; DataOps now uses local `bank_dataops/reports` |
| `mlops-data` | 10 GiB | Retained historical PVC; MLOps now uses local train/evaluate/output folders |

Each Deployment has one replica and uses `Recreate` to avoid overlapping old/new flow servers or SQLite database servers during updates. Training and evaluation share the local MLOps directories through the Minikube mount and retain matching pod affinity. The two pods' combined resource requests must fit on that node. Do not scale these deployments without redesigning file ownership, locking, and persistence. The configuration targets a local Minikube assignment/demo cluster, with single-instance PostgreSQL and SQLite-backed Prefect/MLflow.

ConfigMaps and Secrets supplied through environment variables require a restart of the affected Deployments after edits. Bootstrap scripts are mounted from a ConfigMap but run only at pod initialization:

```bash
kubectl apply -f kubernetes/
kubectl -n bank-marketing rollout restart deployment/bank-dataops deployment/bank-mlops-train deployment/bank-mlops-evaluate
```

## Troubleshooting and shutdown

```bash
kubectl -n bank-marketing get events --sort-by=.metadata.creationTimestamp
kubectl -n bank-marketing describe pods
kubectl -n bank-marketing logs deployment/bank-mlops-train -c wait-for-services
kubectl get storageclass
```

Pending PVCs usually require a working default StorageClass or matching pre-provisioned volumes. Some classes bind only when a consumer pod is scheduled. `ImagePullBackOff` for an application usually means its image was not loaded into the cluster or the registry reference/credentials need updating. A dependency-check init failure can be investigated using the init container logs and server pod logs.

To stop workloads while retaining claims and their data:

```bash
kubectl -n bank-marketing scale deployment --all --replicas=0
```

Restore the declared single replicas with `kubectl apply -f kubernetes/`. Deleting the namespace or running `kubectl delete -f kubernetes/` deletes PVCs and may permanently delete their backing storage depending on the StorageClass reclaim policy. Back up data before intentionally removing persistent storage.

References: [Kubernetes volume access modes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes) and [MLflow artifact proxy configuration](https://www.mlflow.org/docs/latest/self-hosting/architecture/tracking-server/).
