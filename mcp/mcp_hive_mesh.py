#!/usr/bin/env python3
"""
MCP Server: hive-mesh
Monitor and manage all machines in the Hive mesh (5 machines).

Tools:
  - mesh_status: Check all 5 machines (ping + basic health)
  - gpu_status: nvidia-smi on a specified machine
  - ollama_models: List Ollama models on a machine
  - disk_usage: df -h on a machine
  - ram_usage: free -m on a machine
"""

import subprocess
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    name="hive-mesh",
    instructions="Monitor and manage all 5 machines in the Hive mesh: "
    "ZeroDESK (local), ZeroQ (coordinator), ZeroZI (GPU inference), "
    "ZeroNovo (CPU worker), ZeroG7 (training node).",
)

# Machine definitions
MACHINES = {
    "zerodesk": {
        "ip": "100.77.113.48",
        "role": "LLM specialist hub (local)",
        "gpu": "GTX 1660S 6GB",
        "ssh": None,  # local machine
    },
    "zeroq": {
        "ip": "100.70.226.103",
        "role": "Coordinator, 239+ services",
        "gpu": "RTX 5070 Ti 12GB",
        "ssh": "ssh zero@100.70.226.103",
    },
    "zerozi": {
        "ip": "100.105.160.106",
        "role": "GPU inference + training",
        "gpu": "RTX 5060 8GB",
        "ssh": "sshpass -p hivepass ssh -o StrictHostKeyChecking=no zero@100.105.160.106",
    },
    "zeronovo": {
        "ip": "100.103.183.91",
        "role": "CPU worker (content gen)",
        "gpu": "none",
        "ssh": "sshpass -p hivepass ssh -o StrictHostKeyChecking=no zero@100.103.183.91",
    },
    "zerog7": {
        "ip": "100.75.90.82",
        "role": "Training node",
        "gpu": "unknown",
        "ssh": "sshpass -p hivepass ssh -o StrictHostKeyChecking=no zero@100.75.90.82",
    },
}

SSH_TIMEOUT = 30


def _resolve_machine(machine: str) -> dict | None:
    """Resolve a machine name to its config. Case-insensitive, partial match."""
    machine_lower = machine.lower().strip()
    # Exact match
    if machine_lower in MACHINES:
        return MACHINES[machine_lower]
    # Partial match
    for name, config in MACHINES.items():
        if machine_lower in name or name in machine_lower:
            return config
    return None


def _run_on_machine(machine_name: str, command: str, timeout: int = SSH_TIMEOUT) -> tuple[str, str, int]:
    """Run a command on a machine. Returns (stdout, stderr, returncode)."""
    machine = _resolve_machine(machine_name)
    if machine is None:
        return "", f"Unknown machine: {machine_name}. Valid: {', '.join(MACHINES.keys())}", 1

    try:
        if machine["ssh"] is None:
            # Local machine (ZeroDESK)
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        else:
            # Remote machine via SSH
            full_cmd = f"{machine['ssh']} {command!r}"
            result = subprocess.run(
                full_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", f"Command timed out after {timeout}s on {machine_name}", 1
    except Exception as e:
        return "", f"Error on {machine_name}: {str(e)}", 1


def _ping_machine(ip: str) -> tuple[bool, float]:
    """Ping a machine. Returns (reachable, latency_ms)."""
    try:
        result = subprocess.run(
            ["ping", "-c", "1", "-W", "3", ip],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            # Parse latency from "time=X.XX ms"
            for part in result.stdout.split():
                if part.startswith("time="):
                    latency = float(part.split("=")[1])
                    return True, latency
            return True, 0.0
        return False, 0.0
    except Exception:
        return False, 0.0


@mcp.tool(
    name="mesh_status",
    description="Check the health of all 5 machines in the Hive mesh. "
    "Pings each machine, checks SSH connectivity, and reports basic health (uptime, load).",
)
def mesh_status() -> str:
    """Check all machines in the mesh."""
    lines = ["=== Hive Mesh Status ===\n"]

    for name, config in MACHINES.items():
        reachable, latency = _ping_machine(config["ip"])
        status_icon = "UP" if reachable else "DOWN"
        latency_str = f"{latency:.1f}ms" if reachable else "N/A"

        line = f"  {name:<12} ({config['ip']})  {status_icon}  ping={latency_str}  role={config['role']}"

        if reachable:
            # Try to get load average
            stdout, stderr, rc = _run_on_machine(name, "uptime", timeout=10)
            if rc == 0 and stdout.strip():
                # Extract load average
                uptime_str = stdout.strip()
                line += f"\n{'':>14}uptime: {uptime_str}"

        lines.append(line)
        lines.append("")

    return "\n".join(lines)


@mcp.tool(
    name="gpu_status",
    description="Get GPU status (nvidia-smi) on a specified machine. "
    "Valid machines: zerodesk, zeroq, zerozi, zeronovo, zerog7. "
    "Shows GPU utilization, memory usage, temperature, and running processes.",
)
def gpu_status(machine: str) -> str:
    """Get nvidia-smi output from a machine."""
    resolved = _resolve_machine(machine)
    if resolved is None:
        return f"Unknown machine: {machine}. Valid: {', '.join(MACHINES.keys())}"

    if resolved.get("gpu") == "none":
        return f"{machine} has no GPU."

    stdout, stderr, rc = _run_on_machine(
        machine,
        "nvidia-smi --query-gpu=name,memory.used,memory.total,utilization.gpu,temperature.gpu --format=csv,noheader,nounits 2>/dev/null && echo '---' && nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader,nounits 2>/dev/null",
        timeout=15,
    )

    if rc != 0:
        # Fallback to plain nvidia-smi
        stdout2, stderr2, rc2 = _run_on_machine(machine, "nvidia-smi", timeout=15)
        if rc2 != 0:
            return f"GPU query failed on {machine}: {stderr2 or stderr}. GPU may not be available."
        return f"=== GPU Status: {machine} ===\n{stdout2}"

    parts = stdout.strip().split("---")
    gpu_info = parts[0].strip() if parts else ""
    processes = parts[1].strip() if len(parts) > 1 else ""

    lines = [f"=== GPU Status: {machine} ===\n"]

    if gpu_info:
        fields = [f.strip() for f in gpu_info.split(",")]
        if len(fields) >= 5:
            lines.append(f"  GPU:         {fields[0]}")
            lines.append(f"  Memory:      {fields[1]} / {fields[2]} MiB")
            lines.append(f"  Utilization: {fields[3]}%")
            lines.append(f"  Temperature: {fields[4]}C")
        else:
            lines.append(f"  Info: {gpu_info}")

    if processes:
        lines.append(f"\n  Running processes:")
        for proc_line in processes.split("\n"):
            if proc_line.strip():
                lines.append(f"    {proc_line.strip()}")
    else:
        lines.append(f"\n  No GPU processes running.")

    return "\n".join(lines)


@mcp.tool(
    name="ollama_models",
    description="List all Ollama models installed on a machine. "
    "Valid machines: zerodesk, zeroq, zerozi, zeronovo, zerog7. "
    "Shows model names, sizes, and modification dates.",
)
def ollama_models(machine: str) -> str:
    """List Ollama models on a machine."""
    resolved = _resolve_machine(machine)
    if resolved is None:
        return f"Unknown machine: {machine}. Valid: {', '.join(MACHINES.keys())}"

    stdout, stderr, rc = _run_on_machine(
        machine,
        "curl -s http://localhost:11434/api/tags",
        timeout=15,
    )

    if rc != 0 or not stdout.strip():
        # Fallback to ollama list
        stdout2, stderr2, rc2 = _run_on_machine(machine, "ollama list", timeout=15)
        if rc2 != 0:
            return f"Cannot reach Ollama on {machine}: {stderr2 or stderr}"
        return f"=== Ollama Models: {machine} ===\n{stdout2}"

    try:
        import json
        data = json.loads(stdout)
        models = data.get("models", [])
        if not models:
            return f"No Ollama models on {machine}."

        lines = [f"=== Ollama Models: {machine} ({len(models)} total) ===\n"]
        for m in sorted(models, key=lambda x: x.get("name", "")):
            name = m.get("name", "?")
            size_bytes = m.get("size", 0)
            size_gb = size_bytes / (1024**3)
            modified = m.get("modified_at", "?")
            if isinstance(modified, str) and "T" in modified:
                modified = modified.split("T")[0]
            lines.append(f"  {name:<40} {size_gb:.1f} GB  {modified}")

        return "\n".join(lines)
    except (json.JSONDecodeError, Exception) as e:
        return f"=== Ollama Models: {machine} (raw) ===\n{stdout[:2000]}"


@mcp.tool(
    name="disk_usage",
    description="Get disk usage (df -h) on a specified machine. "
    "Valid machines: zerodesk, zeroq, zerozi, zeronovo, zerog7.",
)
def disk_usage(machine: str) -> str:
    """Get disk usage on a machine."""
    resolved = _resolve_machine(machine)
    if resolved is None:
        return f"Unknown machine: {machine}. Valid: {', '.join(MACHINES.keys())}"

    stdout, stderr, rc = _run_on_machine(
        machine,
        "df -h --output=source,size,used,avail,pcent,target -x tmpfs -x devtmpfs -x squashfs 2>/dev/null || df -h",
        timeout=15,
    )

    if rc != 0:
        return f"Failed to get disk usage on {machine}: {stderr}"

    return f"=== Disk Usage: {machine} ===\n{stdout}"


@mcp.tool(
    name="ram_usage",
    description="Get RAM and swap usage (free -m) on a specified machine. "
    "Valid machines: zerodesk, zeroq, zerozi, zeronovo, zerog7.",
)
def ram_usage(machine: str) -> str:
    """Get RAM usage on a machine."""
    resolved = _resolve_machine(machine)
    if resolved is None:
        return f"Unknown machine: {machine}. Valid: {', '.join(MACHINES.keys())}"

    stdout, stderr, rc = _run_on_machine(
        machine,
        "free -m && echo '---' && head -1 /proc/meminfo",
        timeout=15,
    )

    if rc != 0:
        return f"Failed to get RAM usage on {machine}: {stderr}"

    return f"=== RAM Usage: {machine} ===\n{stdout}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
