# Reset, start, run, and verify the Kubernetes stack

Run from a Bash terminal in `/home/haroon/Downloads/scalable_system_assignment`.
Use Kubernetes for this procedure; the Compose projects are separate stacks with separate databases and Prefect state.

## 1. Reset

Choose **one** reset option. Option A retains data and is the normal restart procedure used during this repair.

### A. Restart while retaining databases, models, and reports

```bash
cd /home/haroon/Downloads/scalable_system_assignment
kubectl config use-context minikube
kubectl -n bank-marketing scale deployment --all --replicas=0
kubectl -n bank-marketing wait --for=delete pod --all --timeout=180s
```

### B. Start with empty application storage (destructive)

This deletes the bank namespace, its Secret, PostgreSQL tables/data, Prefect history, MLflow models, and PVC contents. Local DataOps and MLOps folders remain on your computer. Back up anything needed first. It does not delete the Minikube cluster or unrelated namespaces.

```bash
cd /home/haroon/Downloads/scalable_system_assignment
kubectl config use-context minikube
kubectl delete namespace bank-marketing --ignore-not-found --wait=true --timeout=300s
```

For a complete local cluster reset instead, `minikube delete -p minikube` deletes **all** workloads and data in that Minikube profile. Run the startup steps below afterwards. This is optional, and was not needed for the repair.

## 2. Start Minikube and build/load the correctly named images

```bash
minikube start -p minikube --cpus=4 --memory=6144 --disk-size=60g
kubectl config use-context minikube
# With the Docker driver, also update an existing profile's container limit.
# This was needed here: the existing container was limited to 3 GiB.
docker update --memory=6g --memory-swap=8g minikube
minikube config set memory 6144
minikube addons enable dashboard
minikube addons enable metrics-server

docker build -t bank-dataops:1.0.2 ./bank_dataops
docker build -t bank-mlops:1.0.2 ./bank_mlops
minikube image load --daemon --overwrite bank-dataops:1.0.2
minikube image load --daemon --overwrite bank-mlops:1.0.2
minikube image ls
```

The name is `bank-dataops` with a **hyphen**. Both applications use tag `1.0.2`, including shared-folder move support. Loading `bank_dataops` does not satisfy the image reference in the manifests and causes `Init:ImagePullBackOff`.
The `--daemon --overwrite` flags load directly from the host Docker daemon. When changing code again, prefer a new tag and update all three image references in each application manifest.
The memory update assumes the local Docker driver and the `minikube` profile/container name. `minikube config set` supplies the default for future cluster creation; `docker update` changes the running container immediately. This machine has about 12 GiB total RAM.

## 3. Connect the local application folders, then start the six services

DataOps directly mounts local `bank_dataops/data` and `bank_dataops/reports`. MLOps directly mounts local `bank_mlops/train_model`, `bank_mlops/evaluate_model`, and `bank_mlops/outputs`. Install the background mount services once; they start with your user session and retry if Minikube is not available yet:

```bash
systemctl --user enable --now "$PWD/kubernetes/bank-dataops-mount.service"
systemctl --user enable --now "$PWD/kubernetes/bank-mlops-mount.service"
systemctl --user restart bank-dataops-mount.service bank-mlops-mount.service
journalctl --user -u bank-dataops-mount.service -u bank-mlops-mount.service -n 30 --no-pager
minikube -p minikube ssh -- 'findmnt /mnt/bank-dataops'
minikube -p minikube ssh -- 'findmnt /mnt/bank-mlops'
```

Wait until both logs say `Successfully mounted` and both `findmnt` commands show a `9p` mount, then apply the manifests below. The units are configured for this machine's repository location and user ID 1000. If moving the repository or using another user, update their `ExecStart` first and run `systemctl --user daemon-reload`. After a cluster/mount restart, restart the application deployments after the mounts are ready so their volume bindings are refreshed. See [Minikube mounting](https://minikube.sigs.k8s.io/docs/handbook/mount/).

Create credentials only when the Secret is absent. Retain the existing Secret when retaining PostgreSQL storage.

```bash
kubectl apply -f kubernetes/00-namespace.yaml
kubectl apply -f kubernetes/01-config.yaml
if ! kubectl -n bank-marketing get secret bank-secrets >/dev/null 2>&1; then
  BANK_DB_PASSWORD=$(openssl rand -hex 24)
  kubectl -n bank-marketing create secret generic bank-secrets \
    --from-literal=POSTGRES_PASSWORD="$BANK_DB_PASSWORD" \
    --from-literal=DATABASE_URL="postgresql+psycopg2://bank:${BANK_DB_PASSWORD}@bank-postgres:5432/bank_data"
  unset BANK_DB_PASSWORD
fi
kubectl apply -f kubernetes/
kubectl config set-context minikube --namespace=bank-marketing

# Force all processes to pick up loaded images/config, including on a rerun.
kubectl -n bank-marketing rollout restart deployment
for service in bank-postgres bank-prefect bank-mlflow bank-dataops bank-mlops-train bank-mlops-evaluate; do
  kubectl -n bank-marketing rollout status "deployment/$service" --timeout=900s || break
done
kubectl -n bank-marketing get deployments,pods,svc,pvc
```

Expect six deployments with one available replica each, six Running/Ready application pods, and six Bound PVCs. Initial dependency checks and graceful shutdowns can take a few minutes. Never regenerate the database Secret against an existing PostgreSQL volume.

## 4. Open the dashboards

Keep each of these commands running in its **own terminal**. A deployment restart replaces pods and ends their port forwards; rerun the commands afterwards. Stop an existing forward with Ctrl+C before starting another on the same port. Stop Compose's Prefect/MLflow services if they own ports 4200/5000.

```bash
kubectl -n bank-marketing port-forward service/bank-prefect 4200:4200
```

Prefect: <http://localhost:4200/deployments>. Expect these three deployments:

- `bank-marketing-dataops/every-2-minutes`
- `bank-marketing-train-model/manual-training`
- `bank-marketing-evaluate-model/every-2-minutes`

```bash
kubectl -n bank-marketing port-forward service/bank-mlflow 5000:5000
```

MLflow: <http://localhost:5000>. Training and evaluation experiments and the `champion` model alias appear after step 5.

```bash
kubectl -n kubernetes-dashboard port-forward service/kubernetes-dashboard 9090:80
```

Kubernetes workloads: <http://localhost:9090/#/workloads?namespace=bank-marketing>.
Select **bank-marketing** in the dashboard namespace selector. The default namespace contains no bank workloads. CPU/memory charts need the metrics-server addon and may take a minute to populate.
Alternatively run `minikube dashboard --url` and append `#/workloads?namespace=bank-marketing` to the printed URL.

## 5. Process real data through all three deployed pipelines

### Normal use: drop a CSV into incoming and wait

No terminal command or manual flow trigger is required per file. With Minikube, the mount service, Prefect, and DataOps running, place a complete CSV into:

`/home/haroon/Downloads/scalable_system_assignment/bank_dataops/data/incoming`

Every 120 seconds the scheduled DataOps deployment scans that folder. It ingests new CSV contents into PostgreSQL, creates an HTML report in local `bank_dataops/reports`, and moves the CSV from local incoming to local `bank_dataops/data/archive`. Invalid CSVs move to local `bank_dataops/data/failed`; check the Prefect logs for the error. Processing time is additional to the wait until the next scheduled run.

| Local folder | The same folder inside the DataOps pod |
| --- | --- |
| `bank_dataops/data/incoming` | `/data/incoming` |
| `bank_dataops/data/processing` | `/data/processing` |
| `bank_dataops/data/archive` | `/data/archive` |
| `bank_dataops/data/failed` | `/data/failed` |
| `bank_dataops/reports` | `/reports` |

Previously completed CSV contents are not inserted twice, even if renamed. They are archived as duplicates; if their report is missing, the updated flow recreates it. Finish large copies under a temporary filename without a `.csv` suffix, then rename to `.csv`, so the scanner sees a complete file. Use the shared-folder setup with one DataOps deployment. The old upload helper is unnecessary in this configuration.

Optional commands to observe processing:

```bash
kubectl -n bank-marketing logs -f deployment/bank-dataops
systemctl --user status bank-dataops-mount.service --no-pager
```

### Normal MLOps use: ad hoc training and scheduled evaluation

| Pipeline | Local input folder | When it runs | Successful input moves to | Local results |
| --- | --- | --- | --- | --- |
| Training | `bank_mlops/train_model/incoming` | Only when triggered in Prefect | `bank_mlops/train_model/processed` | Model and training metrics in MLflow; training records/metrics in PostgreSQL |
| Evaluation | `bank_mlops/evaluate_model/incoming` | Every 120 seconds | `bank_mlops/evaluate_model/processed` | Prediction CSVs in `bank_mlops/outputs`; evaluation metrics in MLflow/PostgreSQL |

For training, put complete labeled CSVs in local `train_model/incoming`, then open Prefect, select `bank-marketing-train-model/manual-training`, and click **Run**. A training run combines all new pending files. Training has **no schedule**; adding a file alone does not start it. Only one training execution is served at a time.

For evaluation, place labeled CSVs in local `evaluate_model/incoming` and wait for the next scheduled run. It uses the saved MLflow champion without retraining. No upload command, download command, or terminal trigger is needed. Successful inputs move to the respective local `processed` folder automatically. Failed MLOps inputs remain in incoming for retry; inspect Prefect logs. Completed checksums are deduplicated; if a prediction CSV for an already evaluated file/model version is missing, evaluation recreates it. Use observations not used in model training when measuring model performance.

Both local MLOps folders and the pod paths refer to the same files: `/mlops/train_model`, `/mlops/evaluate_model`, and `/mlops/outputs`. Avoid running Compose MLOps against these same directories concurrently. Old `mlops-data` PVC contents remain as historical backups.

### Optional: run the complete DataOps/training/evaluation demo

```bash
python3 kubernetes/check_stack.py
```

This optional demo uses the repository's full 41,188-row CSV, creates a reproducible stratified 80/20 training/holdout split, copies the inputs to the shared folders, and calls Prefect `run_deployment` for each flow in order. Each call waits up to 900 seconds and requires Completed state. It is intended for a clean demo with no unrelated pending inputs. Evaluation starts after training has registered a champion. Training internally compares two algorithms using its own 70/30 split. Inputs are deduplicated on reruns; to train on new data, supply a new CSV checksum or deliberately use the destructive reset.

The MLOps image must include the updated `app/registry.py`: newer MLflow versions use skops serialization, which requires the explicit NumPy/sklearn type allow-list for our locally trained Random Forest. Rebuilding and loading the image in step 2 includes this fix. See the [MLflow sklearn API](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.sklearn.html).

The script uses these command forms (the exact commands and split logic are in `check_stack.py`):

```bash
kubectl -n bank-marketing get pod -l app=bank-dataops -o jsonpath='{.items[0].metadata.name}'
# Resolve a pod name for each pipeline, then copy the relevant CSV:
# kubectl -n bank-marketing cp LOCAL.csv POD:/data/incoming/input.csv
# kubectl -n bank-marketing cp TRAIN.csv TRAIN_POD:/mlops/train_model/incoming/training.csv
# kubectl -n bank-marketing cp HELD_OUT.csv EVAL_POD:/mlops/evaluate_model/incoming/held-out.csv

kubectl -n bank-marketing exec deployment/bank-dataops -- python -c \
  'from prefect.deployments import run_deployment; r=run_deployment("bank-marketing-dataops/every-2-minutes", timeout=900); print(r.state); assert r.state.is_completed()'
kubectl -n bank-marketing exec deployment/bank-mlops-train -- python -c \
  'from prefect.deployments import run_deployment; r=run_deployment("bank-marketing-train-model/manual-training", timeout=900); print(r.state); assert r.state.is_completed()'
kubectl -n bank-marketing exec deployment/bank-mlops-evaluate -- python -c \
  'from prefect.deployments import run_deployment; r=run_deployment("bank-marketing-evaluate-model/every-2-minutes", timeout=900); print(r.state); assert r.state.is_completed()'
```

DataOps and evaluation poll every 120 seconds; training is manual. These are long-lived `flow.serve()` deployments, so Kubernetes Jobs/CronJobs and separate Prefect workers are not expected. An empty-input Completed run does not demonstrate data processing. The verifier requires stored records, training/evaluation metrics, predictions, reports, and a registered champion.

## 6. Verify PostgreSQL, tables, and pipeline results

```bash
python3 kubernetes/check_stack.py --verify-only
cat kubernetes/evidence/verification.json
kubectl -n bank-marketing exec deployment/bank-postgres -- pg_isready -U bank -d bank_data
kubectl -n bank-marketing exec deployment/bank-postgres -- psql -U bank -d bank_data -c '\dt public.*'
kubectl -n bank-marketing exec deployment/bank-postgres -- psql -U bank -d bank_data -c '\dt mlops.*'
kubectl -n bank-marketing exec deployment/bank-postgres -- psql -U bank -d bank_data -c \
  'SELECT batch_id,filename,status,row_count,report_path FROM public.data_batches;'
kubectl -n bank-marketing exec deployment/bank-postgres -- psql -U bank -d bank_data -c \
  'SELECT file_id,kind,filename,status,row_count FROM mlops.files; SELECT algorithm,model_version,metrics FROM mlops.training_runs; SELECT file_id,model_version,metrics FROM mlops.evaluation_metrics; SELECT count(*) AS predictions FROM mlops.predictions;'
kubectl top pods -n bank-marketing
```

Expected application tables:

| Schema | Tables |
| --- | --- |
| public | data_batches, bank_marketing_records |
| mlops | files, training_records, evaluation_records, training_runs, evaluation_metrics, predictions |

For a SQL client, start another forward:

```bash
kubectl -n bank-marketing port-forward service/bank-postgres 5432:5432
```

Connect to host `localhost`, port `5432`, user `bank`, database `bank_data`, with the password stored in Secret `bank-secrets`. Use port `5433:5432` if 5432 is occupied. The Compose PostgreSQL container is a separate database.

Download outputs:

```bash
BANK_DATAOPS_POD=$(kubectl -n bank-marketing get pod -l app=bank-dataops -o jsonpath='{.items[0].metadata.name}')
BANK_EVAL_POD=$(kubectl -n bank-marketing get pod -l app=bank-mlops-evaluate -o jsonpath='{.items[0].metadata.name}')
kubectl -n bank-marketing cp "$BANK_DATAOPS_POD:/reports/." ./kubernetes-reports
kubectl -n bank-marketing cp "$BANK_EVAL_POD:/mlops/outputs/." ./kubernetes-outputs
```

## 7. Troubleshoot or stop

```bash
kubectl -n bank-marketing get events --sort-by=.metadata.creationTimestamp
kubectl -n bank-marketing describe pods
kubectl -n bank-marketing logs deployment/bank-dataops --tail=80
kubectl -n bank-marketing logs deployment/bank-mlops-train --tail=80
kubectl -n bank-marketing logs deployment/bank-mlops-evaluate --tail=80
kubectl -n bank-marketing logs deployment/bank-dataops -c wait-for-services
kubectl -n bank-marketing logs deployment/bank-prefect --tail=80
kubectl -n bank-marketing logs deployment/bank-mlflow --tail=80
```

Stop while retaining data:

```bash
kubectl -n bank-marketing scale deployment --all --replicas=0
systemctl --user stop bank-dataops-mount.service
systemctl --user stop bank-mlops-mount.service
minikube stop -p minikube
```

Restart using steps 2–4. During normal use, just add CSVs to the local incoming folder. Run step 6 only when you want a verification report.

## Verified result from this repair

Verified on 12 November 2026 (Asia/Kolkata); current machine-readable results are in `kubernetes/evidence/verification.json`. Evidence files live in a subdirectory so `kubectl apply -f kubernetes/` reads only manifests.

- All six application deployments available; all six PVCs Bound.
- All three Prefect deployments READY with successful deployed runs.
- PostgreSQL `bank_data`: all eight application tables present, 41,188 DataOps records, 32,951 training records, and 8,237 evaluation records/predictions.
- DataOps HTML report: `/reports/batch_0001_report.html` in the DataOps pod.
- Champion: RandomForest, model version `1`; evaluation accuracy approximately 87.34%, ROC AUC 0.7899.
- Prediction CSV: `/mlops/outputs/evaluation_2_v1.csv` in the evaluation pod.
- No unfinished/failed input batches or files in the application database.
- Prefect and MLflow health endpoints HTTP 200; Kubernetes dashboard API shows six running deployments with no errors; `kubectl top pods` reports metrics.

Earlier failed diagnostic attempts remain in Prefect/MLflow history. They are retained for traceability; the subsequent successful runs and database statuses demonstrate the repaired state.

### Automatic local-folder verification

After connecting the local folders, the automatically scheduled Prefect run `influential-rattlesnake` completed at 02:13 AM local time. It processed `automatic_folder_check.csv` (200 rows, batch 3) and `bank-additional-full2.csv` (41,188 rows, batch 4), created local `batch_0003_report.html` and `batch_0004_report.html`, and moved both CSVs into local archive. No manual flow trigger or upload helper was used. Prefect confirms `auto_scheduled: true`; see `kubernetes/evidence/local-folder-verification.json`. Full stack verification also passed afterwards.

### MLOps local-folder verification

The automatically scheduled evaluation run `romantic-coucal` completed at 02:23 AM local time. It processed a 200-row sample from the previously held-out evaluation data, moved `automatic_evaluation_check.csv` into local `bank_mlops/evaluate_model/processed`, and wrote 200 predictions to local `bank_mlops/outputs/evaluation_3_v1.csv`. Prefect confirms `auto_scheduled: true` and the active 120-second evaluation schedule.

Training has no schedule and its deployment is READY. The existing full training CSV remains unchanged in local `bank_mlops/train_model/incoming`; it was intentionally left pending for the user's next ad hoc Prefect run. Both pods' shared-folder move operations were verified against the actual Minikube mount. Evidence is in `kubernetes/evidence/mlops-local-folder-verification.json`.
