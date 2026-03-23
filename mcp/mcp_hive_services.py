#!/usr/bin/env python3
"""
MCP Server: hive-services
Controls and monitors Hive systemd services on ZeroQ via SSH.

Tools:
  - list_services: List all hive-* services with status
  - restart_service: Restart a specific hive-* service
  - service_logs: Get recent journal logs for a service
  - service_status: Detailed status of one service
  - deploy_file: SCP a file to ZeroQ
"""

import subprocess
import shlex
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    name="hive-services",
    instructions="Control and monitor Hive services on ZeroQ (100.70.226.103). "
    "Provides tools to list, restart, inspect logs, and deploy files to the coordinator.",
)

ZEROQ = "zero@100.70.226.103"
SSH_TIMEOUT = 30


def _ssh_zeroq(command: str, timeout: int = SSH_TIMEOUT) -> tuple[str, str, int]:
    """Run a command on ZeroQ via SSH. Returns (stdout, stderr, returncode)."""
    try:
        result = subprocess.run(
            ["ssh", ZEROQ, command],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", f"SSH command timed out after {timeout}s", 1
    except Exception as e:
        return "", f"SSH error: {str(e)}", 1


@mcp.tool(
    name="list_services",
    description="List all hive-* systemd services on ZeroQ with their current status (running/dead/failed).",
)
def list_services() -> str:
    """List all hive-* services on ZeroQ."""
    stdout, stderr, rc = _ssh_zeroq(
        "systemctl list-units 'hive-*' --type=service --all --no-pager --no-legend"
    )
    if rc != 0 and not stdout:
        return f"Error listing services: {stderr}"

    # Parse and format nicely
    lines = [line.strip() for line in stdout.strip().split("\n") if line.strip()]
    if not lines:
        return "No hive-* services found."

    running = 0
    failed = 0
    other = 0
    output_lines = []
    for line in lines:
        parts = line.split()
        if len(parts) >= 4:
            name = parts[0].replace(".service", "")
            # active/inactive is column 3, sub-state (running/dead/failed) is column 4
            active = parts[2] if len(parts) > 2 else "?"
            sub = parts[3] if len(parts) > 3 else "?"
            if sub == "running":
                running += 1
            elif sub == "failed" or active == "failed":
                failed += 1
            else:
                other += 1
            output_lines.append(f"  {name:<40} {active:<10} {sub}")

    header = f"=== Hive Services on ZeroQ ===\n"
    header += f"Running: {running} | Failed: {failed} | Other: {other} | Total: {len(lines)}\n\n"
    return header + "\n".join(output_lines)


@mcp.tool(
    name="restart_service",
    description="Restart a specific hive-* service on ZeroQ. Provide the service name without '.service' suffix (e.g., 'hive-nerve').",
)
def restart_service(name: str) -> str:
    """Restart a hive service on ZeroQ."""
    # Validate the name to prevent injection
    if not name.startswith("hive-"):
        name = f"hive-{name}"
    safe_name = shlex.quote(name)

    # Restart the service
    stdout, stderr, rc = _ssh_zeroq(f"sudo systemctl restart {safe_name}")
    if rc != 0:
        return f"Failed to restart {name}: {stderr}"

    # Get the new status to confirm
    stdout2, stderr2, rc2 = _ssh_zeroq(
        f"systemctl is-active {safe_name} && systemctl show {safe_name} --property=MainPID,ActiveEnterTimestamp --no-pager"
    )
    return f"Restarted {name} successfully.\n{stdout2.strip()}"


@mcp.tool(
    name="service_logs",
    description="Get recent systemd journal logs for a hive service on ZeroQ. "
    "Provide service name (e.g., 'hive-nerve') and optional number of lines (default 50).",
)
def service_logs(name: str, lines: int = 50) -> str:
    """Get recent logs for a hive service."""
    if not name.startswith("hive-"):
        name = f"hive-{name}"
    safe_name = shlex.quote(name)
    lines = min(max(lines, 1), 500)  # Clamp between 1 and 500

    stdout, stderr, rc = _ssh_zeroq(
        f"journalctl -u {safe_name} -n {lines} --no-pager --output=short-iso",
        timeout=45,
    )
    if rc != 0 and not stdout:
        return f"Error getting logs for {name}: {stderr}"
    if not stdout.strip():
        return f"No logs found for {name}. Service may not exist or have no journal entries."
    return f"=== Last {lines} log lines for {name} ===\n{stdout}"


@mcp.tool(
    name="service_status",
    description="Get detailed systemd status of a specific hive service on ZeroQ, "
    "including PID, memory, uptime, and recent log output.",
)
def service_status(name: str) -> str:
    """Get detailed status of a hive service."""
    if not name.startswith("hive-"):
        name = f"hive-{name}"
    safe_name = shlex.quote(name)

    stdout, stderr, rc = _ssh_zeroq(
        f"systemctl status {safe_name} --no-pager -l",
        timeout=30,
    )
    # systemctl status returns non-zero for inactive/failed services, but still has output
    if not stdout.strip():
        return f"No status info for {name}: {stderr}"
    return f"=== Status: {name} ===\n{stdout}"


@mcp.tool(
    name="deploy_file",
    description="Deploy (SCP) a local file from ZeroDESK to ZeroQ at the specified remote path. "
    "Example: deploy_file('/tmp/patch.py', '/THE_HIVE/agents/core/patch.py')",
)
def deploy_file(local_path: str, remote_path: str) -> str:
    """SCP a file to ZeroQ."""
    try:
        result = subprocess.run(
            ["scp", local_path, f"{ZEROQ}:{remote_path}"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            return f"SCP failed: {result.stderr}"
        return f"Deployed {local_path} -> ZeroQ:{remote_path} successfully."
    except subprocess.TimeoutExpired:
        return "SCP timed out after 60s. File may be too large or network is slow."
    except Exception as e:
        return f"Deploy error: {str(e)}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
