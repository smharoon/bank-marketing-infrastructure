"""Run real deployed flows with disjoint train/evaluation inputs, then verify.

Run from the repository root: python3 kubernetes/check_stack.py
Use --verify-only to inspect an already populated stack without submitting work.
Requires only Python's standard library locally; dependencies run inside pods.
"""
import csv
import json
import random
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NS = "bank-marketing"


def kubectl(*args):
    return subprocess.check_output(
        ["kubectl", "-n", NS, *args], text=True, cwd=ROOT
    ).strip()


def python_in(deployment, code):
    return kubectl("exec", "deployment/" + deployment, "--", "python", "-c", code)


def copy_to(deployment, source, destination):
    pod = kubectl("get", "pod", "-l", "app=" + deployment,
                  "-o", "jsonpath={.items[0].metadata.name}")
    temporary = destination + "." + uuid.uuid4().hex + ".upload"
    kubectl("cp", str(source), pod + ":" + temporary)
    kubectl("exec", pod, "--", "python", "-c",
            "import os,sys; os.replace(sys.argv[1],sys.argv[2])", temporary, destination)


def run_flow(deployment, name):
    print("Running deployed flow:", name, flush=True)
    print(python_in(deployment,
        "from prefect.deployments import run_deployment; "
        f"r=run_deployment({name!r}, timeout=900, poll_interval=5); "
        "print(str(r.id), r.state); "
        "assert r.state.is_completed(), str(r.state)"), flush=True)


if "--verify-only" not in sys.argv:
    source = ROOT / "datasets/bank-additional-full.csv"
    if not source.is_file():
        source = ROOT / "bank_mlops/train_model/incoming/bank-additional-full.csv"
    with source.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        columns, rows = reader.fieldnames, list(reader)
    # Fixed stratified holdout; evaluation observations never enter training.
    train, evaluation = [], []
    rng = random.Random(42)
    for label in ("no", "yes"):
        group = [row for row in rows if row["y"] == label]
        rng.shuffle(group)
        cut = max(1, int(len(group) * 0.2))
        evaluation.extend(group[:cut])
        train.extend(group[cut:])
    with tempfile.TemporaryDirectory(prefix="bank-k8s-check-") as folder:
        for name, records in (("training.csv", train), ("held-out.csv", evaluation)):
            path = Path(folder) / name
            with path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns, delimiter=";")
                writer.writeheader()
                writer.writerows(records)
        print(f"DataOps: {len(rows)} rows; training: {len(train)}; holdout: {len(evaluation)}", flush=True)
        copy_to("bank-dataops", source, "/data/incoming/bank-full.csv")
        run_flow("bank-dataops", "bank-marketing-dataops/every-2-minutes")
        copy_to("bank-mlops-train", Path(folder) / "training.csv", "/mlops/train_model/incoming/training.csv")
        run_flow("bank-mlops-train", "bank-marketing-train-model/manual-training")
        copy_to("bank-mlops-evaluate", Path(folder) / "held-out.csv", "/mlops/evaluate_model/incoming/held-out.csv")
        run_flow("bank-mlops-evaluate", "bank-marketing-evaluate-model/every-2-minutes")

evidence = {}
evidence["deployments"] = json.loads(kubectl("get", "deployments", "-o", "json"))
assert len(evidence["deployments"]["items"]) == 6
for deployment in evidence["deployments"]["items"]:
    assert deployment["status"].get("availableReplicas") == 1, deployment["metadata"]["name"]
evidence["pvc"] = json.loads(kubectl("get", "pvc", "-o", "json"))
assert len(evidence["pvc"]["items"]) == 6
assert all(p["status"]["phase"] == "Bound" for p in evidence["pvc"]["items"])
sql = """
SELECT json_build_object(
 'database', current_database(),
 'tables', (SELECT json_agg(table_schema || '.' || table_name ORDER BY table_schema, table_name)
            FROM information_schema.tables WHERE table_schema IN ('public','mlops') AND table_type='BASE TABLE'),
 'data_batches', (SELECT count(*) FROM public.data_batches WHERE status='completed'),
 'data_records', (SELECT count(*) FROM public.bank_marketing_records),
 'training_records', (SELECT count(*) FROM mlops.training_records),
 'evaluation_records', (SELECT count(*) FROM mlops.evaluation_records),
 'training_runs', (SELECT count(*) FROM mlops.training_runs),
 'evaluation_metrics', (SELECT count(*) FROM mlops.evaluation_metrics),
 'predictions', (SELECT count(*) FROM mlops.predictions),
 'unfinished_batches', (SELECT count(*) FROM public.data_batches WHERE status <> 'completed'),
 'unfinished_files', (SELECT count(*) FROM mlops.files WHERE status <> 'completed'));
"""
evidence["postgres"] = json.loads(kubectl("exec", "deployment/bank-postgres", "--",
    "psql", "-U", "bank", "-d", "bank_data", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql))
assert len(evidence["postgres"]["tables"]) == 8
for name in ("data_batches", "data_records", "training_records", "evaluation_records",
             "training_runs", "evaluation_metrics", "predictions"):
    assert evidence["postgres"][name] > 0, name
assert evidence["postgres"]["unfinished_batches"] == 0
assert evidence["postgres"]["unfinished_files"] == 0
evidence["prefect"] = json.loads(python_in("bank-mlops-train", """
import json, os
from urllib.request import Request, urlopen
base = os.environ['PREFECT_API_URL']
def post(path, data):
    return json.load(urlopen(Request(base+path, data=json.dumps(data).encode(), headers={'Content-Type':'application/json'})))
deployments = post('/deployments/filter', {})
result = []
for d in deployments:
    runs = post('/flow_runs/filter', {'deployments': {'id': {'any_': [d['id']]}}, 'flow_runs': {'state': {'type': {'any_': ['COMPLETED']}}}, 'sort':'END_TIME_DESC', 'limit':1})
    assert runs, d['entrypoint']
    assert d['status'] == 'READY', d['entrypoint']
    result.append({'name':d['name'], 'entrypoint':d['entrypoint'], 'status':d['status'], 'last_completed_run':runs[0]['id']})
assert len(result) == 3
print(json.dumps(result))
"""))
evidence["champion"] = json.loads(python_in("bank-mlops-train", """
import json
from app.registry import configure, MODEL_NAME, MlflowClient
configure()
v = MlflowClient().get_model_version_by_alias(MODEL_NAME, 'champion')
print(json.dumps({'name':v.name, 'version':v.version, 'run_id':v.run_id}))
"""))
evidence["artifacts"] = {
    "reports": python_in("bank-dataops", "from pathlib import Path; p=list(Path('/reports').glob('*.html')); assert p; print('\\n'.join(str(x) for x in p))"),
    "predictions": python_in("bank-mlops-evaluate", "from pathlib import Path; p=list(Path('/mlops/outputs').glob('*.csv')); assert p; print('\\n'.join(str(x) for x in p))"),
}
from datetime import datetime, timezone
evidence["checked_at"] = datetime.now(timezone.utc).isoformat()
# Keep the evidence compact and avoid including environment configuration.
evidence["deployments"] = [{"name": d["metadata"]["name"], "available": d["status"].get("availableReplicas", 0)} for d in evidence["deployments"]["items"]]
evidence["pvc"] = [{"name": p["metadata"]["name"], "phase": p["status"]["phase"]} for p in evidence["pvc"]["items"]]
output = ROOT / "kubernetes/evidence/verification.json"
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(evidence, indent=2) + "\n")
print(json.dumps(evidence, indent=2))
print("All checks passed. Evidence:", output)
