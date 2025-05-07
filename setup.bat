@echo off
REM Start of Windows setup
goto :windows

:unix
#!/bin/bash
echo "Running Unix setup..."

# Check if a command exists
command_exists() {
    command -v "$1" &> /dev/null
}

# Install a package if not already installed
install_package() {
    local package_name=$1
    echo "Checking for $package_name installation..."
    if ! command_exists "$package_name"; then
        echo "$package_name is not installed. Installing $package_name..."
        if command_exists apt; then
            sudo apt update && sudo apt install -y "$package_name"
        elif command_exists yum; then
            sudo yum install -y "$package_name"
        elif command_exists dnf; then
            sudo dnf install -y "$package_name"
        elif command_exists pacman; then
            sudo pacman -Sy --noconfirm "$package_name"
        elif command_exists brew; then
            brew install "$package_name"
        else
            echo "Unsupported package manager. Please install $package_name manually."
            exit 1
        fi
    else
        echo "$package_name is already installed."
    fi
}

# Install Python if not already installed
install_python() {
    echo "Checking for Python installation..."
    if ! command_exists python3; then
        echo "Python is not installed. Installing Python..."
        # MacOS Installation
        if [[ "$OSTYPE" == "darwin"* ]]; then
            if ! command_exists brew; then
                echo "Homebrew not found. Installing Homebrew..."
                /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
                export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
            fi
            brew install python
        elif command_exists apt; then
            sudo apt update && sudo apt install -y python3 python3-pip
        elif command_exists yum; then
            sudo yum install -y python3
        elif command_exists dnf; then
            sudo dnf install -y python3
        elif command_exists pacman; then
            sudo pacman -Sy --noconfirm python python-pip
        else
            echo "Unsupported package manager. Please install Python manually."
            exit 1
        fi
    else
        echo "Python is already installed."
    fi
}

# Install UV CLI
install_uv() {
    echo "Checking for UV installation..."
    if ! command_exists uv; then
        echo "UV is not installed. Installing UV..."
        if command_exists pipx; then
            pipx install uv
        else
            pip install --user uv
        fi
        echo "UV installed successfully."
    else
        echo "UV is already installed."
    fi
}

# Install Git LFS
install_git_lfs() {
    echo "Checking for Git LFS installation..."
    if ! command_exists git-lfs; then
        echo "Git LFS is not installed. Installing Git LFS..."
        if command_exists brew; then
            brew install git-lfs
        elif command_exists apt; then
            curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | sudo bash
            sudo apt install -y git-lfs
        elif command_exists yum; then
            curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.rpm.sh | sudo bash
            sudo yum install -y git-lfs
        elif command_exists dnf; then
            curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.rpm.sh | sudo bash
            sudo dnf install -y git-lfs
        elif command_exists pacman; then
            sudo pacman -Sy --noconfirm git-lfs
        else
            echo "Unsupported package manager. Please install Git LFS manually."
            exit 1
        fi
        git lfs install
        echo "Git LFS installed and initialized successfully."
    else
        echo "Git LFS is already installed."
        git lfs install
    fi
}

# Set up the project
setup_project() {
    echo "Setting up project dependencies with UV..."
    uv sync || { echo "Failed to sync dependencies."; exit 1; }
    echo "Dependencies synced."
    echo "Setup complete."
    echo "To activate the virtual environment, run:"
    echo "  source .venv/bin/activate"
    pre-commit install
}

# Main Unix execution
install_python
install_uv
# install_git_lfs
setup_project
exit 0

:windows
@echo off
echo "Running Windows setup..."

REM Check if Python is installed, if not, download and prompt manual install
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo "Python is not installed. Downloading installer..."
    powershell -Command "Invoke-WebRequest -Uri https://www.python.org/ftp/python/3.10.0/python-3.10.0-amd64.exe -OutFile python_installer.exe"
    if exist python_installer.exe (
        echo "Installer downloaded. Please run it and select ‘Add Python to PATH’."
        start python_installer.exe
        pause
        del python_installer.exe
        where python >nul 2>&1 || (echo "Python install failed."; exit /b 1)
    ) else (
        echo "Download failed."; exit /b 1
    )
) else (
    echo "Python is already installed."
)

python --version || (echo "Python verification failed."; exit /b 1)

REM Check for UV installation
where uv >nul 2>&1
if %errorlevel% neq 0 (
    echo "UV is not installed. Installing via pip..."
    where pip >nul 2>&1 || (echo "pip not found. Please install pip."; exit /b 1)
    pip install --user uv || (echo "UV install failed."; exit /b 1)
) else (
    echo "UV is already installed."
)

REM Ensure user's local bin is on PATH
IF EXIST %USERPROFILE%\AppData\Roaming\Python\Python310\Scripts (
    SET "PATH=%PATH%;%USERPROFILE%\AppData\Roaming\Python\Python310\Scripts"
)

REM Verify UV
uv --version || (echo "UV verification failed."; exit /b 1)

REM Set up the project with UV
echo "Setting up project dependencies with UV..."
uv sync
if %errorlevel% neq 0 (
    echo "Failed to sync dependencies."
    exit /b 1
)
echo "Dependencies synced."

REM Install the local package in editable mode
echo "Installing local package..."
uv pip install -e .
if %errorlevel% neq 0 (
    echo "Failed to install local package."
    exit /b 1
)
echo "Local package installed."

REM Activate the virtual environment
if exist .venv\Scripts\activate.bat (
    echo "Activating .venv..."
    call .venv\Scripts\activate
) else (
    echo ".venv not found. You can activate manually after running 'uv sync'."
)

REM Install pre-commit hooks
pre-commit install

echo "Setup complete."
exit /b 0