# Windows Setup: Hadoop / winutils Prerequisites

PySpark relies on Hadoop binaries (notably `winutils.exe` and `hadoop.dll`) to perform filesystem operations on Windows. Without them, writing or reading Parquet files (including the data preparation step) fails with: "java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset."


Follow the steps below **before running any notebook or streaming job**.

## 1. Prerequisites

- Windows 10 or Windows 11.
- PowerShell (Administrator mode recommended for the environment variables).
- Java 17 or 21 installed and available on `PATH` (`java -version` must succeed).

## 2. Install winutils and configure the environment

Open **PowerShell**, preferably **as Administrator**, and run the following
commands. They download the Hadoop 3.4.0 Windows binaries, place them in
`C:\hadoop\bin`, and register the required environment variables.

```powershell
# Create the target directory
New-Item -ItemType Directory -Force -Path C:\hadoop\bin

# Download the winutils bundle
$url = "https://github.com/kontext-tech/winutils/archive/refs/heads/master.zip"
Invoke-WebRequest $url -OutFile "$env:TEMP\winutils.zip"

# Extract and copy the Hadoop 3.4.0 Windows binaries
Expand-Archive "$env:TEMP\winutils.zip" "$env:TEMP\winutils" -Force
Copy-Item "$env:TEMP\winutils\winutils-master\hadoop-3.4.0-win10-x64\bin\*" "C:\hadoop\bin\" -Recurse -Force

# Register HADOOP_HOME for the current user
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")

# Add C:\hadoop\bin to the user PATH (only if not already present)
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "C:\hadoop\bin") {
    [Environment]::SetEnvironmentVariable("Path", "$userPath;C:\hadoop\bin", "User")
}

# Register HADOOP_HOME at machine scope (requires Administrator)
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "Machine")