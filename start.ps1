# ==========================================
# Database Migration Platform - Startup Script
# ==========================================

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Database Migration Platform" -ForegroundColor Cyan
Write-Host "  Starting Local Development Environment" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Check if .env file exists
if (-not (Test-Path ".env")) {
    Write-Host "ERROR: .env file not found!" -ForegroundColor Red
    Write-Host "Please copy .env.example to .env and configure your PostgreSQL password" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Required configuration:" -ForegroundColor Yellow
    Write-Host "  METADATA_DB_PASSWORD=your_postgres_password" -ForegroundColor White
    Write-Host ""
    exit 1
}

# Function to cleanup services
function Stop-AllServices {
    Write-Host "`nShutting down services..." -ForegroundColor Yellow
    
    if ($global:ApiProcess -and !$global:ApiProcess.HasExited) {
        Stop-Process -Id $global:ApiProcess.Id -Force -ErrorAction SilentlyContinue
        Write-Host "✓ API Server stopped" -ForegroundColor Green
    }
    
    if ($global:UiProcess -and !$global:UiProcess.HasExited) {
        Stop-Process -Id $global:UiProcess.Id -Force -ErrorAction SilentlyContinue
        Write-Host "✓ UI Server stopped" -ForegroundColor Green
    }
    
    Write-Host "✓ All services stopped" -ForegroundColor Green
}

# Trap Ctrl+C
$null = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
    Stop-AllServices
}

try {
    # Step 1: Check PostgreSQL Connection
    Write-Host "[1/4] Checking PostgreSQL connection..." -ForegroundColor Yellow
    
    # Load .env file
    Get-Content .env | ForEach-Object {
        if ($_ -match '^([^#][^=]+)=(.*)$') {
            [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
        }
    }
    
    $pgHost = $env:METADATA_DB_HOST
    $pgPort = $env:METADATA_DB_PORT
    $pgDb = $env:METADATA_DB_NAME
    $pgUser = $env:METADATA_DB_USER
    $pgPassword = $env:METADATA_DB_PASSWORD
    
    if (-not $pgPassword -or $pgPassword -eq 'your_password_here') {
        Write-Host "ERROR: Please set METADATA_DB_PASSWORD in .env file" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "  PostgreSQL: ${pgHost}:${pgPort}/${pgDb}" -ForegroundColor Cyan
    Write-Host "  User: ${pgUser}" -ForegroundColor Cyan
    Write-Host "  ✓ Configuration loaded" -ForegroundColor Green
    
    # Step 2: Initialize Database Schema
    Write-Host "`n[2/4] Initializing database schema..." -ForegroundColor Yellow
    
    $env:PGPASSWORD = $pgPassword
    $schemaCheck = psql -h $pgHost -p $pgPort -U $pgUser -d $pgDb -c "\dt" 2>&1
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Creating schema..." -ForegroundColor Cyan
        psql -h $pgHost -p $pgPort -U $pgUser -d $pgDb -f schema.sql 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  ✓ Schema created successfully" -ForegroundColor Green
        } else {
            Write-Host "  ⚠ Schema may already exist or check permissions" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  ✓ Schema already exists" -ForegroundColor Green
    }
    
    # Step 3: Start Backend API
    Write-Host "`n[3/4] Starting Backend API..." -ForegroundColor Yellow
    
    # Check if virtual environment exists
    if (Test-Path "venv/Scripts/activate.ps1") {
        . venv/Scripts/activate.ps1
        Write-Host "  Using existing virtual environment" -ForegroundColor Cyan
    } elseif (Test-Path "env/Scripts/activate.ps1") {
        . env/Scripts/activate.ps1
        Write-Host "  Using existing virtual environment" -ForegroundColor Cyan
    } else {
        Write-Host "  No virtual environment found, using system Python" -ForegroundColor Yellow
    }
    
    # Install dependencies if needed
    $requirementsChanged = -not (Test-Path ".requirements_installed")
    if ($requirementsChanged -or -not (Get-Command uvicorn -ErrorAction SilentlyContinue)) {
        Write-Host "  Installing Python dependencies..." -ForegroundColor Cyan
        pip install -r requirements.txt --quiet
        New-Item -ItemType File -Path ".requirements_installed" -Force | Out-Null
    }
    
    # Start API server in background
    Write-Host "  Starting FastAPI server on http://localhost:8000" -ForegroundColor Cyan
    $global:ApiProcess = Start-Process -FilePath "python" -ArgumentList "-m", "uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload" -NoNewWindow -PassThru
    Start-Sleep -Seconds 3
    
    # Check if API is responding
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8000/health" -TimeoutSec 5 -ErrorAction Stop
        Write-Host "  ✓ API Server is running" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠ API may still be starting up..." -ForegroundColor Yellow
    }
    
    # Step 4: Start Frontend UI
    Write-Host "`n[4/4] Starting Frontend UI..." -ForegroundColor Yellow
    
    Set-Location ui
    
    # Install node dependencies if needed
    if (-not (Test-Path "node_modules") -or -not (Test-Path ".dependencies_installed")) {
        Write-Host "  Installing Node.js dependencies..." -ForegroundColor Cyan
        npm install --silent
        New-Item -ItemType File -Path ".dependencies_installed" -Force | Out-Null
    }
    
    # Start UI dev server in background
    Write-Host "  Starting Vite dev server on http://localhost:3000" -ForegroundColor Cyan
    $global:UiProcess = Start-Process -FilePath "npm" -ArgumentList "run", "dev" -NoNewWindow -PassThru
    Start-Sleep -Seconds 5
    
    Set-Location ..
    
    Write-Host "`n========================================" -ForegroundColor Green
    Write-Host "  🚀 All Services Running!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "  📱 Web UI:       http://localhost:3000" -ForegroundColor Cyan
    Write-Host "  🔌 API:          http://localhost:8000" -ForegroundColor Cyan
    Write-Host "  📚 API Docs:     http://localhost:8000/docs" -ForegroundColor Cyan
    Write-Host "  🗄️  PostgreSQL:   ${pgHost}:${pgPort}" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Press Ctrl+C to stop all services" -ForegroundColor Yellow
    Write-Host "========================================`n" -ForegroundColor Green
    
    # Keep script running
    while ($true) {
        Start-Sleep -Seconds 1
        
        # Check if processes are still running
        if ($global:ApiProcess.HasExited) {
            Write-Host "`nERROR: API Server stopped unexpectedly!" -ForegroundColor Red
            break
        }
        
        if ($global:UiProcess.HasExited) {
            Write-Host "`nERROR: UI Server stopped unexpectedly!" -ForegroundColor Red
            break
        }
    }
    
} catch {
    Write-Host "`nERROR: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host $_.ScriptStackTrace -ForegroundColor Red
} finally {
    Stop-AllServices
}
