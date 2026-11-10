#!/usr/bin/env python3
"""Run small ClickHouse/ADBC experiments on a shared single-host workstation."""

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import sys
import xml.etree.ElementTree as ET


REPO = Path(__file__).resolve().parents[1]
WORK = REPO / "build/local-experiments"
CONFIG = WORK / "config.json"
GIB = 1024**3


def cpu_groups():
    """Group the CPUs this process may use by physical core."""
    groups = {}
    for cpu in sorted(os.sched_getaffinity(0)):
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        try:
            package = int((topology / "physical_package_id").read_text())
            core = int((topology / "core_id").read_text())
        except (FileNotFoundError, ValueError):
            package, core = 0, cpu
        groups.setdefault((package, core), []).append(cpu)
    return sorted(groups.values(), key=lambda group: min(group))


def memory_bytes():
    values = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        name, _, amount = line.partition(":")
        if name in ("MemTotal", "MemAvailable"):
            values[name] = int(amount.split()[0]) * 1024
    budget = min(values["MemTotal"], values["MemAvailable"])
    # Respect an effective cgroup cap when one is present.
    cap_file = Path("/sys/fs/cgroup/memory.max")
    if cap_file.exists():
        cap = cap_file.read_text().strip()
        if cap != "max":
            budget = min(budget, int(cap))
    return budget


def plan():
    groups = cpu_groups()
    if len(groups) < 4:
        raise RuntimeError("At least four available physical cores are needed for separated local runs")
    reserve = max(2, (len(groups) + 2) // 3)
    per_engine = (len(groups) - reserve) // 2
    if per_engine < 1:
        raise RuntimeError("Not enough physical cores after reserving desktop capacity")
    clickhouse_groups = groups[:per_engine]
    duckdb_groups = groups[per_engine : 2 * per_engine]
    reserved_groups = groups[2 * per_engine :]
    available_gib = memory_bytes() // GIB
    clickhouse_gib = min(16, available_gib // 4)
    duckdb_gib = clickhouse_gib * 3 // 4
    if duckdb_gib < 2:
        raise RuntimeError("At least 8 GiB of available memory is needed for local runs")
    return {
        "clickhouse_cpus": sorted(cpu for group in clickhouse_groups for cpu in group),
        "duckdb_cpus": sorted(cpu for group in duckdb_groups for cpu in group),
        "reserved_cpus": sorted(cpu for group in reserved_groups for cpu in group),
        "threads": per_engine,
        "clickhouse_memory_gib": clickhouse_gib,
        "duckdb_memory_gib": duckdb_gib,
        "available_memory_gib": available_gib,
    }


def paths():
    clickhouse = os.environ.get("CLICKHOUSE_BIN") or shutil.which("clickhouse")
    return {
        "clickhouse_bin": str(Path(clickhouse).resolve()) if clickhouse else "",
        "duckdb_bin": str(REPO / "build/release/duckdb"),
        "extension": str(REPO / "build/release/extension/adbc/adbc.duckdb_extension"),
        "driver_path": str(Path(os.environ.get("ADBC_DRIVER_PATH", Path.home() / ".config/adbc/drivers")).resolve()),
        "profile_uri": "profile://./build/local-experiments/clickhouse.toml",
        "url": "http://127.0.0.1:8123/",
    }


def require_files(config):
    for key in ("clickhouse_bin", "duckdb_bin", "extension"):
        path = Path(config[key]) if config[key] else None
        if not path or not path.exists():
            raise RuntimeError(f"Missing {key}: {config[key] or '(not found)'}")
    for key in ("clickhouse_bin", "duckdb_bin"):
        if not os.access(config[key], os.X_OK):
            raise RuntimeError(f"Not executable: {config[key]}")
    if config["adbc_driver"] == "clickhouse":
        driver_path = Path(config["driver_path"])
        if not driver_path.is_dir() or not any(driver_path.rglob("libadbc_driver_clickhouse.so")):
            raise RuntimeError(f"ClickHouse ADBC driver not found under {driver_path}")
    else:
        library = Path(config["adbc_driver"]).expanduser().resolve()
        if not library.is_file():
            raise RuntimeError(f"ClickHouse ADBC driver not found: {library}")
        config["adbc_driver"] = str(library)


def prepare():
    previous = json.loads(CONFIG.read_text()) if CONFIG.exists() else {}
    driver = os.environ.get("ADBC_EXPERIMENT_DRIVER") or previous.get("adbc_driver", "clickhouse")
    config = {**plan(), **paths(), "adbc_driver": driver}
    overrides = dict(previous.get("memory_overrides_gib", {}))
    for engine in ("clickhouse", "duckdb"):
        raw = os.environ.get(f"LOCAL_{engine.upper()}_MEMORY_GIB")
        if raw is not None:
            value = int(raw)
            if value < 2:
                raise ValueError(f"{engine} memory limit must be at least 2 GiB")
            overrides[engine] = value
        if engine in overrides:
            config[f"{engine}_memory_gib"] = overrides[engine]
    if config["clickhouse_memory_gib"] + config["duckdb_memory_gib"] > config["available_memory_gib"] - 4:
        raise ValueError("Memory limits must leave at least 4 GiB available for the host")
    config["memory_overrides_gib"] = overrides
    require_files(config)
    if Path(config["extension"]).stat().st_mtime < (REPO / "src/adbc_partial_sort.cpp").stat().st_mtime:
        print("Warning: the ADBC binary predates the sort source; run build_local_experiments.sh.", file=sys.stderr)
    WORK.mkdir(parents=True, exist_ok=True)
    (WORK / "clickhouse.toml").write_text(
        'profile_version = 1\ndriver = ' + json.dumps(config["adbc_driver"]) + '\n\n'
        '[Options]\nuri = "http://127.0.0.1:8123/"\n'
    )
    xml = ET.parse(REPO / "clickhouse/clickhouse-config.xml")
    ET.SubElement(xml.getroot(), "max_server_memory_usage").text = str(
        config["clickhouse_memory_gib"] * GIB
    )
    xml.write(WORK / "clickhouse-config.xml", encoding="unicode", xml_declaration=True)
    CONFIG.write_text(json.dumps(config, indent=2) + "\n")
    print_plan(config)
    print(f"ADBC driver: {config['adbc_driver']}")
    print(f"Prepared {CONFIG}")
    return config


def print_plan(config):
    for key in (
        "clickhouse_cpus", "duckdb_cpus", "reserved_cpus", "threads",
        "available_memory_gib", "clickhouse_memory_gib", "duckdb_memory_gib",
    ):
        print(f"{key}: {config[key]}")
    print("CPU affinity is advisory; other desktop processes can still use these CPUs.")


def read_config():
    if not CONFIG.exists():
        raise RuntimeError("Run `python3 scripts/local_experiments.py prepare` first")
    return json.loads(CONFIG.read_text())


def launch(command, cpus, config, dry_run=False, server=False):
    print(shlex.join(command), flush=True)
    print(f"CPU affinity: {','.join(map(str, cpus))}", flush=True)
    if dry_run:
        return
    if not set(cpus).issubset(os.sched_getaffinity(0)):
        raise RuntimeError("Saved CPU plan is no longer available; run prepare again")
    os.sched_setaffinity(0, cpus)
    env = os.environ.copy()
    env.setdefault("ADBC_DRIVER_PATH", config["driver_path"])
    env.setdefault("ADBC_CLICKHOUSE_URL", config["url"])
    env["TZ"] = "UTC"
    if server:
        env["CLICKHOUSE_WATCHDOG_ENABLE"] = "0"
        data_dir = REPO / "clickhouse/clickhouse-data"
        data_dir.mkdir(parents=True, exist_ok=True)
        os.chdir(data_dir)
    else:
        os.chdir(REPO)
    os.execvpe(command[0], command, env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("plan", help="Show CPU and memory plan without writing anything")
    sub.add_parser("prepare", help="Create ignored local configuration and ADBC profile")
    for action in ("server", "load", "benchmark", "run"):
        command = sub.add_parser(action)
        command.add_argument("--dry-run", action="store_true")
        if action == "load":
            command.add_argument("--scale-factor", default="5", help="TPC-H scale factor (default: 5)")
        if action == "run":
            command.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        if args.action == "plan":
            print_plan({**plan(), **paths()})
            return
        if args.action == "prepare":
            prepare()
            return
        config = read_config()
        if args.action == "server":
            launch(
                [config["clickhouse_bin"], "server", f"--config-file={WORK / 'clickhouse-config.xml'}"],
                config["clickhouse_cpus"], config, args.dry_run, server=True,
            )
        elif args.action == "run":
            command = args.command
            if command and command[0] == "--":
                command = command[1:]
            if not command:
                parser.error("run requires a command after --")
            launch(command, config["duckdb_cpus"], config, args.dry_run)
        elif args.action == "load":
            if not args.dry_run and not (REPO / "build/release/extension/tpch/tpch.duckdb_extension").is_file():
                raise RuntimeError("Matching TPC-H extension is missing; run build_local_experiments.sh")
            launch(
                [sys.executable, str(REPO / "scripts/load_clickhouse_tpch.py"),
                 "--duckdb", config["duckdb_bin"], "--scale-factor", args.scale_factor,
                 "--threads", str(config["threads"]), "--memory-limit",
                 f"{config['duckdb_memory_gib']}GiB", "--url", config["url"]],
                config["duckdb_cpus"], config, args.dry_run,
            )
        elif args.action == "benchmark":
            launch(
                [sys.executable, str(REPO / "scripts/benchmark_clickhouse_partial_sort.py"),
                 "--duckdb", config["duckdb_bin"], "--extension", config["extension"],
                 "--profile", config["profile_uri"], "--url", config["url"],
                 "--rows", "100000", "--repeats", "1", "--warmups", "0",
                 "--threads", str(config["threads"]), "--memory-limit",
                 f"{config['duckdb_memory_gib']}GiB", "--output",
                 str(WORK / "partial-sort-benchmark.json")],
                config["duckdb_cpus"], config, args.dry_run,
            )
    except (OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"local_experiments: {error}\n")


if __name__ == "__main__":
    main()
