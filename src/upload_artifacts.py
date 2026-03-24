import json
import os
import duckdb


def upload_metadata():
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("MOTHERDUCK_TOKEN is missing")

    con = duckdb.connect("md:my_voyage_db")
    con.execute("CREATE SCHEMA IF NOT EXISTS operations")

    # 1. Upload Run Results
    if os.path.exists("target/run_results.json"):
        with open("target/run_results.json") as f:
            run_data = json.load(f)

        # Flatten results slightly for easier querying in Evidence
        results = []
        run_id = run_data.get("metadata", {}).get("run_id")
        generated_at = run_data.get("metadata", {}).get("generated_at")

        for res in run_data.get("results", []):
            results.append(
                {
                    "run_id": run_id,
                    "generated_at": generated_at,
                    "unique_id": res.get("unique_id"),
                    "status": res.get("status"),
                    "execution_time": res.get("execution_time"),
                    "rows_affected": res.get("adapter_response", {}).get("rows_affected"),
                    "message": res.get("message"),
                }
            )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS operations.dbt_run_results (
                run_id VARCHAR,
                generated_at TIMESTAMP,
                unique_id VARCHAR,
                status VARCHAR,
                execution_time DOUBLE,
                rows_affected BIGINT,
                message VARCHAR
            )
        """
        )

        con.executemany(
            "INSERT INTO operations.dbt_run_results VALUES (?, ?, ?, ?, ?, ?, ?)",
            [tuple(r.values()) for r in results],
        )
        print(f"✅ Uploaded {len(results)} records to operations.dbt_run_results")


if __name__ == "__main__":
    upload_metadata()
