from __future__ import annotations
import json
from pathlib import Path
from typing import Dict
import pandas as pd
from airflow.exceptions import AirflowFailException

# Lightweight expectations (no heavy framework):
def simple_expectations(df: pd.DataFrame) -> Dict[str, float]:
    """Return summary dict; raise AirflowFailException on violation."""
    issues = []

    # 1) Schema/columns
    if "value" not in df.columns:
        issues.append("missing column: value")

    # 2) Row count sanity
    if len(df) < 10:
        issues.append(f"too few rows: {len(df)} (< 10)")

    # 3) Range checks
    if "value" in df.columns:
        bad_low = (df["value"] < 0).sum()
        bad_high = (df["value"] > 1).sum()
        if bad_low or bad_high:
            issues.append(f"value out-of-range count: <0={bad_low}, >1={bad_high}")

        # basic nulls check
        nulls = int(df["value"].isna().sum())
        if nulls > 0:
            issues.append(f"null values found: {nulls}")

    summary = {
        "rows": float(len(df)),
        "min": float(df["value"].min()) if "value" in df.columns and len(df) > 0 else float("nan"),
        "max": float(df["value"].max()) if "value" in df.columns and len(df) > 0 else float("nan"),
        "mean": float(df["value"].mean()) if "value" in df.columns and len(df) > 0 else float("nan"),
    }

    if issues:
        raise AirflowFailException("Data quality failed: " + "; ".join(issues))

    return summary


def write_json(d: Dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(d, indent=2))


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise AirflowFailException(f"Dataset not found: {path}")
    return pd.read_csv(path)


def drift_report(reference_csv: Path, current_csv: Path, out_dir: Path) -> Dict:
    """
    Build a drift report (HTML + JSON) with Evidently.
    If reference is missing, set current as reference and mark no_drift.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    import pandas as pd
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset

    cur = load_csv(current_csv)

    if not reference_csv.exists():
        # Initialize reference on first run
        cur.to_csv(reference_csv, index=False)
        result = {"initialized_reference": True, "drift_detected": False, "note": "reference created"}
        write_json(result, out_dir / "drift_summary.json")
        # Still produce a 'report' with cur vs cur
        rep = Report(metrics=[DataDriftPreset()])
        rep.run(reference_data=cur, current_data=cur)
        rep.save_html(str(out_dir / "evidently_report.html"))
        return result

    ref = load_csv(reference_csv)

    rep = Report(metrics=[DataDriftPreset()])
    rep.run(reference_data=ref, current_data=cur)
    # Save HTML + JSON
    rep.save_html(str(out_dir / "evidently_report.html"))
    res = rep.as_dict()

    # Quick boolean: did preset flag drift?
    # Different versions of Evidently expose status slightly differently; be conservative.
    drift_detected = False
    try:
        # Search dict for a global 'data_drift' result
        for m in res.get("metrics", []):
            if m.get("metric", "").endswith("DataDriftPreset"):
                # common field names:
                if "result" in m and isinstance(m["result"], dict):
                    if m["result"].get("dataset_drift") is True:
                        drift_detected = True
                        break
    except Exception:
        pass

    summary = {"initialized_reference": False, "drift_detected": drift_detected}
    write_json(summary, out_dir / "drift_summary.json")
    return summary

