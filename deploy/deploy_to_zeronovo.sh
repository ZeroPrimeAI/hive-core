#!/usr/bin/env bash
###############################################################################
# deploy_to_zeronovo.sh — Deploy CPU-based agents to ZeroNovo (100.103.183.91)
#
# Deploys 4 lightweight scraping/tracking agents that need no GPU:
#   - competitive_intel.py   (port 8901)
#   - market_research.py     (port 8902)
#   - revenue_hunter.py      (port 8903)
#   - social_media_manager.py (port 8904)
#
# Usage: ./deploy_to_zeronovo.sh [--dry-run]
#
# Requirements:
#   - sshpass installed on this machine (ZeroDESK)
#   - Agent .py files in /home/zero/hive/agents/ on ZeroDESK
#   - ZeroNovo reachable at 100.103.183.91
###############################################################################

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────

ZERONOVO_IP="100.103.183.91"
ZERONOVO_USER="zero"
ZERONOVO_PASS="hivepass"
REMOTE_AGENT_DIR="/home/zero/hive/agents"
LOCAL_AGENT_DIR="/home/zero/hive/agents"

# SSH/SCP options (password auth via sshpass, skip host key check)
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"
SSH_CMD="sshpass -p '${ZERONOVO_PASS}' ssh ${SSH_OPTS} ${ZERONOVO_USER}@${ZERONOVO_IP}"
SCP_CMD="sshpass -p '${ZERONOVO_PASS}' scp ${SSH_OPTS}"

# Agent definitions: name, file, port, description
declare -A AGENTS
AGENTS=(
    [competitive_intel]="8901|Web scraping competitive intelligence agent"
    [market_research]="8902|Web scraping market research agent"
    [revenue_hunter]="8903|Web scraping revenue hunting agent"
    [social_media_manager]="8904|Social media tracking and management agent"
)

AGENT_FILES=(
    "competitive_intel.py"
    "market_research.py"
    "revenue_hunter.py"
    "social_media_manager.py"
)

REQUIRED_PACKAGES=("fastapi" "uvicorn" "httpx" "beautifulsoup4")

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    echo "=== DRY RUN MODE — no changes will be made ==="
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_err()   { echo -e "${RED}[ERROR]${NC} $*"; }

run_remote() {
    # Execute a command on ZeroNovo
    if $DRY_RUN; then
        echo "  [DRY-RUN] ssh ${ZERONOVO_USER}@${ZERONOVO_IP}: $*"
        return 0
    fi
    sshpass -p "${ZERONOVO_PASS}" ssh ${SSH_OPTS} "${ZERONOVO_USER}@${ZERONOVO_IP}" "$@"
}

run_remote_sudo() {
    # Execute a sudo command on ZeroNovo
    if $DRY_RUN; then
        echo "  [DRY-RUN] ssh ${ZERONOVO_USER}@${ZERONOVO_IP} (sudo): $*"
        return 0
    fi
    sshpass -p "${ZERONOVO_PASS}" ssh ${SSH_OPTS} "${ZERONOVO_USER}@${ZERONOVO_IP}" \
        "echo '${ZERONOVO_PASS}' | sudo -S bash -c '$*'" 2>/dev/null
}

scp_to_remote() {
    local src="$1"
    local dst="$2"
    if $DRY_RUN; then
        echo "  [DRY-RUN] scp ${src} -> ${ZERONOVO_USER}@${ZERONOVO_IP}:${dst}"
        return 0
    fi
    sshpass -p "${ZERONOVO_PASS}" scp ${SSH_OPTS} "${src}" "${ZERONOVO_USER}@${ZERONOVO_IP}:${dst}"
}

# ─── Pre-flight checks ──────────────────────────────────────────────────────

echo ""
echo "============================================================"
echo "  Deploy CPU Agents to ZeroNovo (${ZERONOVO_IP})"
echo "============================================================"
echo ""

# Check sshpass is installed
if ! command -v sshpass &>/dev/null; then
    log_err "sshpass is not installed. Install it: sudo apt install sshpass"
    exit 1
fi
log_ok "sshpass found"

# Check all agent files exist locally
MISSING=0
for f in "${AGENT_FILES[@]}"; do
    if [[ ! -f "${LOCAL_AGENT_DIR}/${f}" ]]; then
        log_err "Missing agent file: ${LOCAL_AGENT_DIR}/${f}"
        MISSING=1
    else
        log_ok "Found: ${LOCAL_AGENT_DIR}/${f}"
    fi
done

if [[ $MISSING -eq 1 ]]; then
    log_err "One or more agent files are missing from ${LOCAL_AGENT_DIR}/"
    log_err "Create the agent files first, then re-run this script."
    exit 1
fi

# Check ZeroNovo is reachable
log_info "Testing connectivity to ZeroNovo (${ZERONOVO_IP})..."
if ! run_remote "echo ok" &>/dev/null; then
    log_err "Cannot reach ZeroNovo at ${ZERONOVO_IP}. Check network/Tailscale."
    exit 1
fi
log_ok "ZeroNovo is reachable"

# ─── Step 1: Check/install Python packages on ZeroNovo ───────────────────────

echo ""
log_info "Step 1: Checking Python packages on ZeroNovo..."

PKGS_TO_INSTALL=()
for pkg in "${REQUIRED_PACKAGES[@]}"; do
    # Map package names to import names for checking
    import_name="${pkg}"
    if [[ "${pkg}" == "beautifulsoup4" ]]; then
        import_name="bs4"
    fi

    if run_remote "python3 -c 'import ${import_name}'" &>/dev/null; then
        log_ok "Package '${pkg}' already installed"
    else
        log_warn "Package '${pkg}' NOT found — will install"
        PKGS_TO_INSTALL+=("${pkg}")
    fi
done

if [[ ${#PKGS_TO_INSTALL[@]} -gt 0 ]]; then
    log_info "Installing missing packages: ${PKGS_TO_INSTALL[*]}"
    run_remote_sudo "pip3 install --break-system-packages ${PKGS_TO_INSTALL[*]}"
    if [[ $? -eq 0 ]]; then
        log_ok "Packages installed successfully"
    else
        log_err "Package installation failed. You may need to install manually."
        exit 1
    fi
else
    log_ok "All required packages already installed"
fi

# ─── Step 2: Check ports are free on ZeroNovo ────────────────────────────────

echo ""
log_info "Step 2: Checking port availability on ZeroNovo..."

PORT_CONFLICT=0
for agent in "${!AGENTS[@]}"; do
    IFS='|' read -r port desc <<< "${AGENTS[$agent]}"
    port_in_use=$(run_remote "ss -tlnp | grep ':${port} ' || true")
    if [[ -n "${port_in_use}" ]]; then
        log_err "Port ${port} is already in use on ZeroNovo (needed for ${agent})"
        log_err "  ${port_in_use}"
        PORT_CONFLICT=1
    else
        log_ok "Port ${port} is free (for ${agent})"
    fi
done

if [[ $PORT_CONFLICT -eq 1 ]]; then
    log_err "Port conflict detected. Pick different ports or stop conflicting services."
    exit 1
fi

# ─── Step 3: Create remote directory and copy agent files ────────────────────

echo ""
log_info "Step 3: Copying agent files to ZeroNovo:${REMOTE_AGENT_DIR}/"

run_remote "mkdir -p ${REMOTE_AGENT_DIR}"

for f in "${AGENT_FILES[@]}"; do
    log_info "  Copying ${f}..."
    scp_to_remote "${LOCAL_AGENT_DIR}/${f}" "${REMOTE_AGENT_DIR}/${f}"
    log_ok "  ${f} deployed"
done

# ─── Step 4: Create systemd service files ────────────────────────────────────

echo ""
log_info "Step 4: Creating systemd service files on ZeroNovo..."

for agent in "${!AGENTS[@]}"; do
    IFS='|' read -r port desc <<< "${AGENTS[$agent]}"
    service_name="hive-${agent//_/-}"
    agent_file="${agent}.py"

    log_info "  Creating ${service_name}.service (port ${port})..."

    SERVICE_CONTENT="[Unit]
Description=Hive ${desc}
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=zero
Group=zero
WorkingDirectory=${REMOTE_AGENT_DIR}
ExecStart=/usr/bin/python3 ${REMOTE_AGENT_DIR}/${agent_file} --port ${port}
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1
Environment=HIVE_AGENT_PORT=${port}
Environment=HIVE_AGENT_NAME=${agent}
Environment=HIVE_NERVE_URL=http://100.70.226.103:8200
Environment=HIVE_ZEROQ_IP=100.70.226.103
Environment=HIVE_ZERODESK_IP=100.77.113.48
Environment=HIVE_ZERONOVO_IP=100.103.183.91
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${service_name}

# Resource limits — be a good citizen
Nice=10
MemoryMax=512M
CPUQuota=50%

[Install]
WantedBy=multi-user.target"

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would write /etc/systemd/system/${service_name}.service"
    else
        # Write service file via sudo
        run_remote_sudo "cat > /etc/systemd/system/${service_name}.service << 'SERVICEEOF'
${SERVICE_CONTENT}
SERVICEEOF"
    fi

    log_ok "  ${service_name}.service created"
done

# ─── Step 5: Reload systemd and start services ──────────────────────────────

echo ""
log_info "Step 5: Reloading systemd and starting services..."

run_remote_sudo "systemctl daemon-reload"
log_ok "systemd daemon reloaded"

for agent in "${!AGENTS[@]}"; do
    IFS='|' read -r port desc <<< "${AGENTS[$agent]}"
    service_name="hive-${agent//_/-}"

    log_info "  Enabling and starting ${service_name}..."
    run_remote_sudo "systemctl enable ${service_name}"
    run_remote_sudo "systemctl start ${service_name}"
    log_ok "  ${service_name} started"
done

# ─── Step 6: Verify all services are running ─────────────────────────────────

echo ""
log_info "Step 6: Verifying services (waiting 5s for startup)..."

if ! $DRY_RUN; then
    sleep 5
fi

FAILED=0
for agent in "${!AGENTS[@]}"; do
    IFS='|' read -r port desc <<< "${AGENTS[$agent]}"
    service_name="hive-${agent//_/-}"

    # Check systemd status
    status=$(run_remote "systemctl is-active ${service_name} 2>/dev/null || echo 'inactive'")
    if [[ "${status}" == "active" ]]; then
        log_ok "${service_name} is RUNNING (port ${port})"
    else
        log_err "${service_name} is ${status}"
        log_warn "  Check logs: ssh zero@${ZERONOVO_IP} 'journalctl -u ${service_name} -n 20 --no-pager'"
        FAILED=1
    fi

    # Check health endpoint (if agent exposes one)
    if ! $DRY_RUN; then
        health=$(run_remote "curl -s --max-time 3 http://localhost:${port}/health 2>/dev/null || echo 'no_health'")
        if [[ "${health}" != "no_health" ]]; then
            log_ok "  Health check passed: ${health}"
        else
            log_warn "  Health endpoint not responding yet (may need more startup time)"
        fi
    fi
done

# ─── Step 7: Verify neighboring services weren't affected (Build Safety Law 5) ─

echo ""
log_info "Step 7: Verifying existing services on ZeroNovo are unaffected..."

EXISTING_SERVICES=("8200:nerve-backup" "8141:dispatch" "8899:failover")
for svc in "${EXISTING_SERVICES[@]}"; do
    IFS=':' read -r check_port check_name <<< "${svc}"
    if ! $DRY_RUN; then
        check=$(run_remote "ss -tlnp | grep ':${check_port} ' || echo 'not_found'")
        if [[ "${check}" != "not_found" ]]; then
            log_ok "Existing service ${check_name} (port ${check_port}) still running"
        else
            log_warn "Existing service ${check_name} (port ${check_port}) not detected — may not be running"
        fi
    else
        echo "  [DRY-RUN] Would check port ${check_port} (${check_name})"
    fi
done

# ─── Summary ─────────────────────────────────────────────────────────────────

echo ""
echo "============================================================"
echo "  Deployment Summary — ZeroNovo (${ZERONOVO_IP})"
echo "============================================================"
echo ""
echo "  Agents deployed:"
for agent in "${!AGENTS[@]}"; do
    IFS='|' read -r port desc <<< "${AGENTS[$agent]}"
    service_name="hive-${agent//_/-}"
    printf "    %-30s port %-5s  %s\n" "${service_name}" "${port}" "${desc}"
done
echo ""
echo "  Agent files: ${REMOTE_AGENT_DIR}/"
echo "  Service files: /etc/systemd/system/hive-*.service"
echo ""
echo "  Useful commands:"
echo "    ssh zero@${ZERONOVO_IP} 'systemctl status hive-competitive-intel'"
echo "    ssh zero@${ZERONOVO_IP} 'journalctl -u hive-revenue-hunter -f'"
echo "    ssh zero@${ZERONOVO_IP} 'systemctl restart hive-market-research'"
echo ""

if [[ $FAILED -eq 1 ]]; then
    log_warn "Some services failed to start. Check logs above."
    exit 1
else
    log_ok "All services deployed and running!"
    echo ""
fi

# ─── Rollback instructions (Build Safety Law 4) ─────────────────────────────

echo "  Rollback (if needed):"
echo "    ssh zero@${ZERONOVO_IP} 'echo hivepass | sudo -S bash -c \""
echo "      systemctl stop hive-competitive-intel hive-market-research hive-revenue-hunter hive-social-media-manager"
echo "      systemctl disable hive-competitive-intel hive-market-research hive-revenue-hunter hive-social-media-manager"
echo "      rm /etc/systemd/system/hive-competitive-intel.service"
echo "      rm /etc/systemd/system/hive-market-research.service"
echo "      rm /etc/systemd/system/hive-revenue-hunter.service"
echo "      rm /etc/systemd/system/hive-social-media-manager.service"
echo "      systemctl daemon-reload"
echo "    \"'"
echo ""
