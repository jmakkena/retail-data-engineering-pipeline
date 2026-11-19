
import os
import sys
import shutil
import subprocess
from pathlib import Path

# ==========================================
# CONFIGURATION
# ==========================================

PROJECT_DIR = Path(__file__).resolve().parent

DATABRICKS_PROFILE = os.getenv(
    "DATABRICKS_PROFILE", "Jay"
)

DATABRICKS_JOB_ID = os.getenv(
    "DATABRICKS_JOB_ID", "479958063523665"
)


# ==========================================
# FIND DATABRICKS CLI
# ==========================================

def find_databricks_cli():

    cli = os.getenv("DATABRICKS_CLI_PATH")

    if cli and Path(cli).is_file():
        return cli

    cli = shutil.which("databricks")

    if cli:
        return cli

    if os.name == "nt":
        winget_folder = (
            Path.home()
            / "AppData"
            / "Local"
            / "Microsoft"
            / "WinGet"
            / "Packages"
        )

        matches = sorted(
            winget_folder.glob(
                "Databricks.DatabricksCLI_*/databricks.exe"
            )
        )

        if matches:
            return str(matches[0])

    raise FileNotFoundError(
        "Databricks CLI not found. "
        "Install the CLI or set DATABRICKS_CLI_PATH."
    )


# ==========================================
# RUN PYTHON SCRIPTS
# ==========================================

def run_script(script_name):

    print(f"\nRunning {script_name}...", flush=True)

    subprocess.run(
        [sys.executable, str(PROJECT_DIR / script_name)],
        cwd=PROJECT_DIR,
        check=True
    )

    print(f"Completed {script_name}", flush=True)


# ==========================================
# RUN DATABRICKS WORKFLOW
# ==========================================

def run_databricks_job():

    cli = find_databricks_cli()

    print("\nStarting Databricks workflow...", flush=True)

    subprocess.run(
        [
            cli,
            "jobs",
            "run-now",
            DATABRICKS_JOB_ID,
            "--profile",
            DATABRICKS_PROFILE
        ],
        cwd=PROJECT_DIR,
        check=True
    )

    print("Databricks workflow completed successfully!")


# ==========================================
# MAIN PIPELINE
# ==========================================

if __name__ == "__main__":

    print("Starting Retail Data Engineering Pipeline")

    # Extract incremental order updates
    run_script("incremental_extract.py")

    # Extract order-items snapshot if changed
    run_script("extract_order_items.py")

    # Upload new CSV and JSON files
    run_script("upload_to_databricks.py")

    # Execute Bronze, Silver, Gold and quality checks
    run_databricks_job()

    print("\nEntire pipeline completed successfully!")
