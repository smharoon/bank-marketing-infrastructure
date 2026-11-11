"""Upload local CSVs to Kubernetes and wait for the deployed DataOps flow.

Usage: python3 kubernetes/submit_dataops.py [CSV ...]
With no arguments, submit bank_dataops/data/incoming/*.csv.
Reports are downloaded locally; completed local incoming files move to archive.
Use --sync-only to retrieve results for files already processed in Kubernetes.
"""
import argparse
import hashlib
import json
import shutil
import subprocess
import uuid
from pathlib import Path


def kubectl(*args, capture=False):
    result = subprocess.run(
        ["kubectl", "-n", "bank-marketing", *args], check=True,
        text=True, stdout=subprocess.PIPE if capture else None,
    )
    return result.stdout.strip() if capture else None


def checksum(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def sync_result(pod, path, expected_checksum, incoming):
    result = json.loads(kubectl("exec", pod, "--", "python", "-c",
        "import json,sys; from sqlalchemy import select; "
        "from app.database import db_engine,batches; "
        "e=db_engine(); "
        "c=e.connect(); "
        "r=c.execute(select(batches.c.batch_id,batches.c.status,batches.c.report_path)"
        ".where(batches.c.file_checksum==sys.argv[1])).mappings().one(); "
        "print(json.dumps(dict(r)))", expected_checksum, capture=True))
    if result["status"] != "completed":
        raise RuntimeError(f"{path.name}: batch {result['batch_id']} is {result['status']}; local input retained")
    remote_report = Path(result["report_path"])
    if remote_report.parent != Path("/reports"):
        raise RuntimeError(f"Unexpected report path: {remote_report}")
    reports = incoming.parents[1] / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    temporary = reports / f"{uuid.uuid4().hex}.download"
    report = reports / remote_report.name
    kubectl("cp", f"{pod}:{remote_report}", str(temporary))
    if report.exists() and report.read_bytes() != temporary.read_bytes():
        report = reports / f"kubernetes_{uuid.uuid4().hex[:8]}_{remote_report.name}"
    temporary.replace(report)
    print(f"Report downloaded: {report}", flush=True)
    if checksum(path) != expected_checksum:
        raise RuntimeError(f"{path} changed during processing; local input retained")
    if path.parent.resolve() == incoming.resolve():
        archive = incoming.parent / "archive"
        archive.mkdir(parents=True, exist_ok=True)
        destination = archive / path.name
        if destination.exists():
            destination = archive / f"{uuid.uuid4().hex[:8]}_{path.name}"
        shutil.move(str(path), str(destination))
        print(f"Completed batch {result['batch_id']}; local input archived: {destination}", flush=True)
    else:
        print(f"Completed batch {result['batch_id']}; source outside local incoming retained: {path}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", nargs="*", type=Path)
    parser.add_argument("--sync-only", action="store_true",
                        help="Download existing completed reports and archive local incoming files without uploading or running flows")
    args = parser.parse_args()
    deployment = json.loads(kubectl("get", "deployment/bank-dataops", "-o", "json", capture=True))
    if any(v.get("hostPath", {}).get("path") == "/mnt/bank-dataops/data"
           for v in deployment["spec"]["template"]["spec"]["volumes"]):
        print("Automatic local-folder watching is enabled. Place CSVs in bank_dataops/data/incoming; "
              "the scheduled pipeline processes them every 2 minutes and writes reports locally. "
              "No upload or manual trigger is needed.")
        return
    incoming = Path(__file__).resolve().parents[1] / "bank_dataops/data/incoming"
    paths = args.csv or sorted(incoming.glob("*.csv"))
    if not paths:
        parser.error(f"No CSV files found in {incoming}")
    for path in paths:
        if not path.is_file() or path.suffix.lower() != ".csv":
            parser.error(f"Expected a CSV file: {path}")
    pod = kubectl("get", "pods", "-l", "app=bank-dataops", "-o",
                  "jsonpath={.items[0].metadata.name}", capture=True)
    submitted = [(path, checksum(path)) for path in paths]
    if args.sync_only:
        for path, digest in submitted:
            sync_result(pod, path, digest, incoming)
        return
    for path in paths:
        # Publish only after the copy completes so the scheduled scanner
        # cannot ingest a partially uploaded CSV.
        temporary = f"/data/incoming/{uuid.uuid4().hex}.upload"
        target = "/data/incoming/" + path.name
        print(f"Uploading {path} to {pod}:{target}", flush=True)
        kubectl("cp", str(path.resolve()), pod + ":" + temporary)
        # Avoid replacing another queued file with the same name. Both paths
        # are on the same PVC, so linking publishes the complete file atomically.
        kubectl("exec", pod, "--", "python", "-c",
                "import os,sys; source,target=sys.argv[1:];\n"
                "try: os.link(source,target)\n"
                "finally: os.unlink(source)", temporary, target)
    kubectl("exec", pod, "--", "python", "-c",
            "from prefect.deployments import run_deployment; "
            "r=run_deployment('bank-marketing-dataops/every-2-minutes', timeout=900, poll_interval=5); "
            "print(r.id, r.state); assert r.state.is_completed(), str(r.state)")
    for path, digest in submitted:
        sync_result(pod, path, digest, incoming)
    print("Deployment run completed; local reports and archive updated.", flush=True)


if __name__ == "__main__":
    main()
