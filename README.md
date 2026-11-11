# Scalable System Assignment

Bank Marketing DataOps and MLOps pipelines using Prefect, PostgreSQL, MLflow, Docker, and Kubernetes.

The local Minikube deployment shares the application folders with the host machine. Files placed in the incoming folders are visible directly to the pipelines, and generated reports and predictions appear locally.

## Pipelines

| Pipeline | Trigger | Local input | Result |
| --- | --- | --- | --- |
| DataOps | Every 2 minutes | `bank_dataops/data/incoming/` | PostgreSQL records, HTML reports in `bank_dataops/reports/`, and source CSVs moved to `data/archive/` |
| Model training | Ad hoc through Prefect | `bank_mlops/train_model/incoming/` | Compares Logistic Regression and Random Forest, registers the champion in MLflow, and moves successful inputs to `train_model/processed/` |
| Model evaluation | Every 2 minutes | `bank_mlops/evaluate_model/incoming/` | Evaluates the saved champion, stores metrics and predictions, writes CSVs to `bank_mlops/outputs/`, and moves inputs to `evaluate_model/processed/` |

Training has no schedule. To train, add labeled CSVs to its incoming folder, open Prefect, and run `bank-marketing-train-model/manual-training`. DataOps and evaluation discover files automatically while the cluster and mount services are running. No per-file upload or download command is needed.

The CSV contract uses semicolon-separated Bank Marketing columns, including the labeled target `y` (`yes` or `no`). Completed inputs are deduplicated by file checksum. Use separate held-out observations for evaluation.

## Start and reset

See **[the complete reset-and-start guide](kubernetes/RESET_AND_START.md)** for:

- Restarting while retaining data, or deliberately resetting application storage.
- Building/loading images and starting all six Kubernetes deployments.
- Enabling the background services that share local DataOps and MLOps folders.
- Opening Prefect, MLflow, and the Kubernetes dashboard.
- Checking PostgreSQL tables and pipeline results.

Requirements: Docker, Minikube, `kubectl`, OpenSSL, and Python 3. The documented local setup uses 4 CPUs and 6 GiB for Minikube. The mount service files target `~/Downloads/scalable_system_assignment`; update their `ExecStart` paths if you clone elsewhere.

Expected dashboards, after starting the documented port forwards:

- Prefect: <http://localhost:4200>
- MLflow: <http://localhost:5000>
- Kubernetes: <http://localhost:9090/#/workloads?namespace=bank-marketing>

## Dataset

The original CSV is kept in [`datasets/`](datasets/) so processing does not remove the repository's master copy. Copy the data you want to process into an appropriate incoming folder. Keep evaluation observations separate from training data.

## Project layout

```text
bank_dataops/     CSV validation, ingestion, EDA and HTML reporting
bank_mlops/       Training, model registration, evaluation and prediction
datasets/        Original example dataset
infrastructure/  Alternative Docker Compose infrastructure
kubernetes/      Manifests, mount services, verification and startup guide
```

Kubernetes is the primary documented deployment. The Compose projects are an alternative; avoid running both against the same local folders concurrently. GitHub stores the source; the services run in your Kubernetes cluster.

## Credentials and runtime files

Real `.env` files, private keys, generated databases, reports, predictions, processed input files, and Python caches are excluded from version control. Safe `.env.example` templates are included. Kubernetes database credentials are generated during setup and stored in a Secret.

For an existing populated cluster, run a read-only verification:

```bash
python3 kubernetes/check_stack.py --verify-only
```

Results are written locally under `kubernetes/evidence/`.
