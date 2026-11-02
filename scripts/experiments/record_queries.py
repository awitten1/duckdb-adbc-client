#!/usr/bin/env python3
"""Measure complete ClickHouse Native/Arrow transfers, optionally recording server perf."""

import argparse
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import time
from urllib.parse import urlencode
import uuid


REPO = Path(__file__).resolve().parents[2]
CASES = ("native_tcp", "http_native", "http_arrow", "http_arrow_lz4")
CLI = str(REPO / "clickhouse/.clickhouse/bin/clickhouse-client")
HTTP_URL = "http://127.0.0.1:8123/"
ENV = dict(os.environ, TZ="UTC", DEBUGINFOD_URLS="")
SQL = "SELECT * FROM tpch_sf25.orders ORDER BY o_orderdate ASC NULLS LAST"
SETTINGS = " SETTINGS max_threads=16, log_processors_profiles=1, log_query_settings=1, use_query_cache=0"


def control(sql):
    result = subprocess.run(
        [CLI, "--format", "JSONEachRow", "--query", sql],
        env=ENV,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


class Recorder:
    def __init__(self, directory, name, pid):
        self.target_pid = pid
        self.prefix = directory / name
        self.ctl = Path(str(self.prefix) + ".ctl")
        self.ack = Path(str(self.prefix) + ".ack")
        self.pidfile = Path(str(self.prefix) + ".pid")
        for path in (self.ctl, self.ack):
            os.mkfifo(path, 0o600)
        self.control_fd = os.open(self.ctl, os.O_RDWR)
        self.ack_fd = os.open(self.ack, os.O_RDWR | os.O_NONBLOCK)
        self.log = Path(str(self.prefix) + ".record.log").open("w")
        command = [
            "perf",
            "record",
            "-e",
            "cycles",
            "-F",
            "199",
            "--call-graph",
            "dwarf,16384",
            "--timestamp",
            "--sample-cpu",
            "-m",
            "256",
            "-D",
            "-1",
            "--control",
            f"fifo:{self.ctl},{self.ack}",
            "-p",
            str(pid),
            "-o",
            str(self.prefix) + ".data",
        ]
        start = "from pathlib import Path; import os,sys; Path(sys.argv[1]).write_text(str(os.getpid())); os.execvp(sys.argv[2],sys.argv[2:])"
        self.process = subprocess.Popen(
            ["sudo", "-n", "python3", "-c", start, str(self.pidfile), *command],
            stdout=self.log,
            stderr=subprocess.STDOUT,
            env=ENV,
        )
        self.stopped = False
        try:
            self.command("enable")
        except BaseException:
            self.stop(disable=False)
            raise

    def command(self, command):
        os.write(self.control_fd, (command + "\n").encode())
        ready, _, _ = select.select([self.ack_fd], [], [], 30)
        if not ready:
            raise RuntimeError(f"perf did not acknowledge {command}: {self.prefix}")
        reply = os.read(self.ack_fd, 4096)
        if b"ack" not in reply:
            raise RuntimeError(f"Unexpected perf control response: {reply!r}")

    def stop(self, disable=True):
        if self.stopped:
            return
        try:
            if self.process.poll() is None:
                try:
                    if disable and Path(f"/proc/{self.target_pid}").exists():
                        self.command("disable")
                finally:
                    # Stop only the perf process created by this recorder, even
                    # if its FIFO acknowledgement failed during startup/cleanup.
                    if self.pidfile.exists():
                        pid = int(self.pidfile.read_text())
                        subprocess.run(
                            ["sudo", "-n", "kill", "-INT", str(pid)], check=True
                        )
                    else:
                        self.process.terminate()
        finally:
            try:
                self.process.wait(timeout=60)
            finally:
                self.log.close()
                for fd in (self.control_fd, self.ack_fd):
                    os.close(fd)
                for path in (self.ctl, self.ack, self.pidfile):
                    path.unlink(missing_ok=True)
                self.stopped = True
                data = Path(str(self.prefix) + ".data")
                if data.exists():
                    subprocess.run(
                        [
                            "sudo",
                            "-n",
                            "chown",
                            f"{os.getuid()}:{os.getgid()}",
                            str(data),
                        ],
                        check=True,
                    )
        if self.process.returncode:
            raise RuntimeError(
                f"perf exited with {self.process.returncode}; see {self.prefix}.record.log"
            )


def workload(case, directory, tag):
    query_id = "perf-" + tag + "-" + uuid.uuid4().hex
    if case == "native_tcp":
        return (
            [
                CLI,
                "--host",
                "127.0.0.1",
                "--compression",
                "0",
                "--query_id",
                query_id,
                "--format",
                "PrettyCompact",
                "--output_format_pretty_max_rows",
                "1000",
                "--query",
                SQL + SETTINGS,
            ],
            query_id,
        )
    if case.startswith("http_"):
        native = case == "http_native"
        params = {
            "query_id": query_id,
            "default_format": "Native" if native else "ArrowStream",
            "compress": "1" if case == "http_arrow_lz4" else "0",
            "output_format_arrow_compression_method": "none",
        }
        if native:
            params["client_protocol_version"] = "54492"
        command = [
            "curl",
            "-fsS",
            "--max-time",
            "180",
            "--output",
            "/dev/null",
            "--write-out",
            "%{json}",
            "--data-binary",
            SQL + SETTINGS,
            HTTP_URL + "?" + urlencode(params),
        ]
        return command, query_id
    raise ValueError("Unsupported format case: " + case)


def main(argv=None):
    global CLI, SQL, SETTINGS, HTTP_URL
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "--cases", nargs="+", choices=CASES, default=["http_native", "http_arrow"]
    )
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--no-warmup", action="store_true")
    parser.add_argument("--clickhouse-client", type=Path, default=Path(CLI))
    parser.add_argument(
        "--url",
        default=HTTP_URL,
        help="ClickHouse HTTP base URL, without query parameters",
    )
    parser.add_argument(
        "--sql-file",
        type=Path,
        help="SELECT without trailing SETTINGS; defaults to the SF25 orders sort",
    )
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--expected-rows", type=int, default=37500000)
    parser.add_argument(
        "--min-network-bytes",
        type=int,
        default=1000000000,
        help="Reject accidentally suppressed transfers (lower for small test datasets)",
    )
    parser.add_argument("--server-service", default="adbc-clickhouse.service")
    parser.add_argument("--server-pid", type=int)
    parser.add_argument(
        "--no-perf",
        action="store_true",
        help="Collect timings and query/processor logs without perf",
    )
    args = parser.parse_args(argv)
    if (
        min(args.repeats, args.threads) < 1
        or min(args.expected_rows, args.min_network_bytes) < 0
    ):
        parser.error(
            "repeats/threads must be positive; row and byte counts must be nonnegative"
        )
    if args.server_pid is not None and args.server_pid < 1:
        parser.error("--server-pid must be positive")
    if "?" in args.url or "#" in args.url:
        parser.error("--url must not contain query parameters or a fragment")
    CLI = str(args.clickhouse_client.resolve())
    HTTP_URL = args.url.rstrip("/") + "/"
    if args.sql_file:
        SQL = args.sql_file.read_text().strip().rstrip(";")
    SETTINGS = f" SETTINGS max_threads={args.threads}, log_processors_profiles=1, log_query_settings=1, use_query_cache=0"
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    server_pid = None
    if not args.no_perf:
        if not sys.platform.startswith("linux"):
            parser.error(
                "perf recording requires Linux; use --no-perf for timings only"
            )
        server_pid = args.server_pid or int(
            subprocess.check_output(
                ["systemctl", "show", args.server_service, "--value", "-p", "MainPID"],
                text=True,
            )
        )
        if server_pid <= 0:
            parser.error("ClickHouse is not running; start it or pass --server-pid")
    (root / "configuration.json").write_text(
        json.dumps(
            {
                key: str(value) if isinstance(value, Path) else value
                for key, value in vars(args).items()
            },
            indent=2,
        )
    )
    results = []
    for case in args.cases:
        rounds = (
            list(range(args.repeats)) if args.no_warmup else [-1, *range(args.repeats)]
        )
        for repeat in rounds:
            others = control(
                "SELECT query_id, query FROM system.processes WHERE query_id != currentQueryID()"
            )
            if others:
                raise RuntimeError("Another query is running: " + str(others))
            warmup = repeat == -1
            tag = case + ("-warmup" if warmup else f"-r{repeat}")
            directory = root / tag
            directory.mkdir()
            command, query_id = workload(case, directory, tag)
            (directory / "command.json").write_text(json.dumps(command, indent=2))
            read_fd, write_fd = os.pipe()
            gate = "import os,sys; fd=int(sys.argv[1]); os.read(fd,1); os.close(fd); os.execvpe(sys.argv[2],sys.argv[2:],os.environ)"
            recorders = []
            with (directory / "stdout").open("wb") as output, (
                directory / "stderr"
            ).open("wb") as errors:
                process = subprocess.Popen(
                    [sys.executable, "-c", gate, str(read_fd), *command],
                    pass_fds=(read_fd,),
                    stdout=output,
                    stderr=errors,
                    env=ENV,
                )
                os.close(read_fd)
                try:
                    if not warmup and not args.no_perf:
                        recorders.append(Recorder(directory, "server", server_pid))
                    started_epoch = time.time()
                    started = time.perf_counter()
                    os.write(write_fd, b"1")
                    os.close(write_fd)
                    write_fd = None
                    _, status, usage = os.wait4(process.pid, 0)
                    process.returncode = os.waitstatus_to_exitcode(status)
                    elapsed = time.perf_counter() - started
                finally:
                    if write_fd is not None:
                        os.close(write_fd)
                    try:
                        for recorder in reversed(recorders):
                            recorder.stop()
                    finally:
                        if process.returncode is None:
                            process.terminate()
                            process.wait(timeout=30)
            if process.returncode:
                raise RuntimeError(
                    f"{tag} failed: {(directory / 'stderr').read_text()}"
                )
            control("SYSTEM FLUSH LOGS")
            where = "query_id='" + query_id + "'"
            queries = control(
                "SELECT query_id,query_duration_ms,read_rows,read_bytes,result_rows,result_bytes,"
                "memory_usage,interface,http_user_agent,Settings,ProfileEvents,query "
                "FROM system.query_log WHERE type='QueryFinish' AND (" + where + ") "
                "ORDER BY query_start_time_microseconds"
            )
            if not queries:
                raise RuntimeError("Missing query-log records for " + tag)
            if query_id:
                assert int(queries[0]["result_rows"]) == args.expected_rows, queries
                assert (
                    int(queries[0]["ProfileEvents"]["NetworkSendBytes"])
                    >= args.min_network_bytes
                ), queries
            ids = ",".join("'" + q["query_id"] + "'" for q in queries)
            processors = control(
                "SELECT * FROM system.processors_profile_log WHERE query_id IN ("
                + ids
                + ")"
            )
            result = {
                "case": case,
                "repeat": repeat,
                "warmup": warmup,
                "directory": str(directory),
                "wall_seconds": elapsed,
                "start_epoch": started_epoch,
                "client_user_seconds": usage.ru_utime,
                "client_system_seconds": usage.ru_stime,
                "client_peak_rss_kib": usage.ru_maxrss,
                "server_queries": queries,
                "processors": processors,
                "server_pid": server_pid,
                "client_pid": process.pid,
            }
            (directory / "result.json").write_text(json.dumps(result, indent=2))
            results.append(result)
            (root / "results.json").write_text(json.dumps(results, indent=2))
            print(
                json.dumps(
                    {
                        k: result[k]
                        for k in (
                            "case",
                            "repeat",
                            "warmup",
                            "wall_seconds",
                            "client_user_seconds",
                            "client_system_seconds",
                        )
                    }
                ),
                flush=True,
            )
    print("ARTIFACTS " + str(root), flush=True)


if __name__ == "__main__":
    main()
