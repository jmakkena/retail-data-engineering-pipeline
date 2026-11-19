
import os
import shutil
import subprocess
from pathlib import Path

# ==========================================
# CONFIGURATION
# ==========================================

PROJECT_DIR = Path(__file__).resolve().parent
LOCAL_FOLDER = PROJECT_DIR / "data" / "incremental"

DATABRICKS_VOLUME = (
    "dbfs:/Volumes/workspace/bronze/retail_raw/"
)

PROFILE = os.getenv("DATABRICKS_PROFILE", "Jay")


# ==========================================
# FIND DATABRICKS CLI
# ==========================================

def find_databricks_cli():

    # First check environment variable
    cli = os.getenv("DATABRICKS_CLI_PATH")

    if cli and Path(cli).is_file():
        return cli

    # Next check system PATH
    cli = shutil.which("databricks")

    if cli:
        return cli

    # Windows fallback for winget installations
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


DATABRICKS_CLI = find_databricks_cli()


# ==========================================
# FIND LOCAL DATA FILES
# ==========================================

if not LOCAL_FOLDER.exists():
    raise FileNotFoundError(
        f"Local data folder not found: {LOCAL_FOLDER}"
    )

data_files = sorted(
    list(LOCAL_FOLDER.glob("*.csv"))
    + list(LOCAL_FOLDER.glob("order_items_*.json"))
)

print(f"Found {len(data_files)} local data files.")


# ==========================================
# CHECK EXISTING DATABRICKS FILES
# ==========================================

result = subprocess.run(
    [
        DATABRICKS_CLI,
        "fs",
        "ls",
        DATABRICKS_VOLUME,
        "--profile",
        PROFILE
    ],
    capture_output=True,
    text=True,
    check=True
)

existing_files = {
    line.strip().split("/")[-1]
    for line in result.stdout.splitlines()
    if line.strip()
}

print(f"Found {len(existing_files)} files in Databricks.")


# ==========================================
# UPLOAD NEW FILES
# ==========================================

uploaded = 0
skipped = 0

# Upload CSVs before JSON metadata.
# The JSON signals that a snapshot is ready.
data_files.sort(key=lambda p: p.suffix == ".json")

for file_path in data_files:

    filename = file_path.name

    if filename in existing_files:
        print(f"Skipping existing file: {filename}")
        skipped += 1
        continue

    destination = DATABRICKS_VOLUME + filename

    print(f"Uploading: {filename}")

    try:
        subprocess.run(
            [
                DATABRICKS_CLI,
                "fs",
                "cp",
                str(file_path.resolve()),
                destination,
                "--profile",
                PROFILE
            ],
            check=True
        )

        print(f"Successfully uploaded: {filename}")
        uploaded += 1

    except subprocess.CalledProcessError as error:
        print(f"Upload failed: {filename}")
        raise RuntimeError(
            f"Failed to upload {filename}"
        ) from error


# ==========================================
# UPLOAD SUMMARY
# ==========================================

print("\nUpload process complete!")
print(f"Files uploaded: {uploaded}")
print(f"Files skipped: {skipped}")
print(f"Total local data files: {len(data_files)}")

import os
import shutil
import subprocess
from pathlib import Path

# ==========================================
# CONFIGURATION
# ==========================================

PROJECT_DIR = Path(__file__).resolve().parent
LOCAL_FOLDER = PROJECT_DIR / "data" / "incremental"

DATABRICKS_VOLUME = (
    "dbfs:/Volumes/workspace/bronze/retail_raw/"
)

PROFILE = os.getenv("DATABRICKS_PROFILE", "Jay")


# ==========================================
# FIND DATABRICKS CLI
# ==========================================

def find_databricks_cli():

    # First check environment variable
    cli = os.getenv("DATABRICKS_CLI_PATH")

    if cli and Path(cli).is_file():
        return cli

    # Next check system PATH
    cli = shutil.which("databricks")

    if cli:
        return cli

    # Windows fallback for winget installations
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


DATABRICKS_CLI = find_databricks_cli()


# ==========================================
# FIND LOCAL DATA FILES
# ==========================================

if not LOCAL_FOLDER.exists():
    raise FileNotFoundError(
        f"Local data folder not found: {LOCAL_FOLDER}"
    )

data_files = sorted(
    list(LOCAL_FOLDER.glob("*.csv"))
    + list(LOCAL_FOLDER.glob("order_items_*.json"))
)

print(f"Found {len(data_files)} local data files.")


# ==========================================
# CHECK EXISTING DATABRICKS FILES
# ==========================================

result = subprocess.run(
    [
        DATABRICKS_CLI,
        "fs",
        "ls",
        DATABRICKS_VOLUME,
        "--profile",
        PROFILE
    ],
    capture_output=True,
    text=True,
    check=True
)

existing_files = {
    line.strip().split("/")[-1]
    for line in result.stdout.splitlines()
    if line.strip()
}

print(f"Found {len(existing_files)} files in Databricks.")


# ==========================================
# UPLOAD NEW FILES
# ==========================================

uploaded = 0
skipped = 0

# Upload CSVs before JSON metadata.
# The JSON signals that a snapshot is ready.
data_files.sort(key=lambda p: p.suffix == ".json")

for file_path in data_files:

    filename = file_path.name

    if filename in existing_files:
        print(f"Skipping existing file: {filename}")
        skipped += 1
        continue

    destination = DATABRICKS_VOLUME + filename

    print(f"Uploading: {filename}")

    try:
        subprocess.run(
            [
                DATABRICKS_CLI,
                "fs",
                "cp",
                str(file_path.resolve()),
                destination,
                "--profile",
                PROFILE
            ],
            check=True
        )

        print(f"Successfully uploaded: {filename}")
        uploaded += 1

    except subprocess.CalledProcessError as error:
        print(f"Upload failed: {filename}")
        raise RuntimeError(
            f"Failed to upload {filename}"
        ) from error


# ==========================================
# UPLOAD SUMMARY
# ==========================================

print("\nUpload process complete!")
print(f"Files uploaded: {uploaded}")
print(f"Files skipped: {skipped}")
print(f"Total local data files: {len(data_files)}")
