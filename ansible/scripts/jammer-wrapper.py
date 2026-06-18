#!/usr/bin/env python3

import argparse
import csv
import json
import os
import re
import signal
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests


TIMING_WARNING_NS = 1_000_000


def wall_time():
    return datetime.now(timezone.utc).isoformat()


def sanitize_run_id(value):
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
    if not sanitized:
        raise ValueError("run id must contain at least one safe character")
    return sanitized


class StateFile:
    def __init__(self, path, mode):
        self.path = Path(path)
        self.mode = mode
        self.started_wall = wall_time()
        self.warnings = 0

    def write(self, status, **extra):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "status": status,
            "mode": self.mode,
            "pid": os.getpid(),
            "started_wall": self.started_wall,
            "updated_wall": wall_time(),
            "updated_monotonic_ns": time.monotonic_ns(),
            "warnings": self.warnings,
            **extra,
        }
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self.path.name}.", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w") as handle:
                json.dump(payload, handle, sort_keys=True)
                handle.write("\n")
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


class TransitionLog:
    fields = [
        "mode",
        "sequence",
        "action",
        "requested_gain",
        "planned_monotonic_ns",
        "request_started_monotonic_ns",
        "completed_monotonic_ns",
        "request_started_wall",
        "completed_wall",
        "http_latency_ns",
        "timing_error_ns",
        "attempts",
        "success",
        "warning",
    ]

    def __init__(self, path, mode, state):
        self.path = Path(path)
        self.mode = mode
        self.state = state
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("w", newline="", buffering=1)
        self.writer = csv.DictWriter(self.handle, fieldnames=self.fields)
        self.writer.writeheader()

    def close(self):
        self.handle.close()

    def record(
        self,
        sequence,
        action,
        requested_gain,
        planned_ns,
        started_ns,
        completed_ns,
        started_wall,
        completed_wall,
        attempts,
        success,
        error="",
    ):
        timing_error_ns = started_ns - planned_ns if planned_ns is not None else 0
        warnings = []
        if abs(timing_error_ns) > TIMING_WARNING_NS:
            warnings.append(f"timing deviation {timing_error_ns} ns exceeds 1 ms")
        if not success:
            warnings.append(error or "transition failed")
        warning = "; ".join(warnings)
        if warning:
            self.state.warnings += 1
        self.writer.writerow(
            {
                "mode": self.mode,
                "sequence": sequence,
                "action": action,
                "requested_gain": "" if requested_gain is None else requested_gain,
                "planned_monotonic_ns": "" if planned_ns is None else planned_ns,
                "request_started_monotonic_ns": started_ns,
                "completed_monotonic_ns": completed_ns,
                "request_started_wall": started_wall,
                "completed_wall": completed_wall,
                "http_latency_ns": completed_ns - started_ns,
                "timing_error_ns": timing_error_ns,
                "attempts": attempts,
                "success": success,
                "warning": warning,
            }
        )


class JammerController:
    def __init__(self, base_url, transition_log, timeout_s=1.0):
        self.base_url = base_url.rstrip("/")
        self.log = transition_log
        self.timeout_s = timeout_s
        self.sequence = 0

    def transition(self, action, gain=None, planned_ns=None):
        self.sequence += 1
        query = urlencode({"gain": gain}) if action == "on" else ""
        url = f"{self.base_url}/{action}"
        if query:
            url = f"{url}?{query}"

        started_ns = time.monotonic_ns()
        started_wall = wall_time()
        success = False
        error = ""
        attempts = 0
        for attempts in (1, 2):
            try:
                response = requests.get(url, timeout=self.timeout_s)
                response.raise_for_status()
                payload = response.json()
                if not payload.get("ok", False):
                    raise RuntimeError(payload.get("error", "API returned ok=false"))
                success = True
                break
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"

        completed_ns = time.monotonic_ns()
        completed_wall = wall_time()
        self.log.record(
            self.sequence,
            action,
            gain,
            planned_ns,
            started_ns,
            completed_ns,
            started_wall,
            completed_wall,
            attempts,
            success,
            error,
        )
        return completed_ns, success


class Runner:
    def __init__(self, args):
        self.args = args
        self.stop_requested = False
        self.last_error = ""
        self.state = StateFile(args.state_file, args.mode)
        self.log = TransitionLog(args.log_file, args.mode, self.state)
        self.controller = JammerController(args.api_url, self.log)

    def request_stop(self, _signum=None, _frame=None):
        self.stop_requested = True

    def wait_until(self, deadline_ns):
        while not self.stop_requested:
            remaining_ns = deadline_ns - time.monotonic_ns()
            if remaining_ns <= 0:
                return True
            if remaining_ns > 1_000_000:
                time.sleep((remaining_ns - 500_000) / 1_000_000_000)
        return False

    def arm_off(self):
        completed_ns, _ = self.controller.transition("off")
        self.state.write("armed", armed_monotonic_ns=completed_ns)
        return completed_ns

    def run_pulse_train(self):
        previous_ns = self.arm_off()
        planned_ns = previous_ns + self.args.start_delay_ms * 1_000_000
        if not self.wait_until(planned_ns):
            return
        for pulse in range(self.args.pulse_count):
            previous_ns, _ = self.controller.transition(
                "on", self.args.gain, planned_ns
            )
            self.state.write("running", pulse=pulse + 1, phase="on")
            planned_ns = previous_ns + self.args.on_duration_ms * 1_000_000
            if not self.wait_until(planned_ns):
                return
            previous_ns, _ = self.controller.transition("off", planned_ns=planned_ns)
            self.state.write("running", pulse=pulse + 1, phase="off")
            if pulse + 1 < self.args.pulse_count:
                planned_ns = previous_ns + self.args.off_duration_ms * 1_000_000
                if not self.wait_until(planned_ns):
                    return
        end_deadline = previous_ns + self.args.end_delay_ms * 1_000_000
        self.wait_until(end_deadline)

    def ramp_gains(self):
        if self.args.steps <= 1:
            return [self.args.start_gain]
        increment = (self.args.end_gain - self.args.start_gain) / (
            self.args.steps - 1
        )
        return [
            round(self.args.start_gain + increment * index, 6)
            for index in range(self.args.steps)
        ]

    def run_ramp(self):
        previous_ns = self.arm_off()
        planned_ns = previous_ns + self.args.start_delay_ms * 1_000_000
        if not self.wait_until(planned_ns):
            return
        while not self.stop_requested:
            for gain in self.ramp_gains():
                previous_ns, _ = self.controller.transition("on", gain, planned_ns)
                self.state.write("running", phase="on", gain=gain)
                planned_ns = previous_ns + int(self.args.on * 1_000_000_000)
                if not self.wait_until(planned_ns):
                    return
                previous_ns, _ = self.controller.transition(
                    "off", planned_ns=planned_ns
                )
                self.state.write("running", phase="off", gain=0)
                planned_ns = previous_ns + int(self.args.off * 1_000_000_000)
                if not self.wait_until(planned_ns):
                    return

    def run_continuous(self):
        completed_ns, _ = self.controller.transition("on", self.args.gain)
        self.state.write("armed", armed_monotonic_ns=completed_ns, gain=self.args.gain)
        while not self.stop_requested:
            time.sleep(0.1)

    def run(self):
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)
        self.state.write("starting")
        exit_code = 0
        try:
            if self.args.mode == "pulse_train":
                self.run_pulse_train()
            elif self.args.mode == "continuous":
                self.run_continuous()
            else:
                self.run_ramp()
        except Exception as exc:
            exit_code = 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.state.warnings += 1
            self.state.write("error", error=self.last_error)
        finally:
            try:
                self.controller.transition("off")
            except Exception as exc:
                self.state.warnings += 1
                print(f"failed to force jammer off: {exc}", file=sys.stderr)
            self.state.write(
                "completed" if exit_code == 0 else "error",
                **({"error": self.last_error} if self.last_error else {}),
            )
            self.log.close()
        return exit_code


def parser():
    result = argparse.ArgumentParser(description="5G measurement jammer scheduler")
    result.add_argument(
        "--mode",
        choices=["ramp", "pulse_train", "continuous"],
        default="ramp",
    )
    result.add_argument("--run-id", default="manual")
    result.add_argument("--api-url", default="http://127.0.0.1:5678")
    result.add_argument("--state-file")
    result.add_argument("--log-file")

    # Legacy ramp arguments remain supported.
    result.add_argument("--start_gain", type=float, default=10.0)
    result.add_argument("--end_gain", type=float, default=50.0)
    result.add_argument("--steps", type=int, default=5)
    result.add_argument("--on", type=float, default=30.0)
    result.add_argument("--off", type=float, default=30.0)

    result.add_argument("--gain", type=float, default=0.0)
    result.add_argument("--pulse-count", type=int, default=1)
    result.add_argument("--start-delay-ms", type=int, default=0)
    result.add_argument("--on-duration-ms", type=int, default=10)
    result.add_argument("--off-duration-ms", type=int, default=10)
    result.add_argument("--end-delay-ms", type=int, default=0)
    return result


def validate_args(args):
    args.run_id = sanitize_run_id(args.run_id)
    if args.state_file is None:
        args.state_file = f"/run/jammer-wrapper/{args.run_id}.json"
    if args.log_file is None:
        args.log_file = f"/tmp/jammer-{args.run_id}.csv"
    if args.start_delay_ms < 0:
        raise ValueError("start_delay_ms must be >= 0")
    if args.mode == "ramp":
        if args.start_gain < 0 or args.end_gain < 0:
            raise ValueError("ramp gains must be >= 0")
        if args.steps < 0:
            raise ValueError("steps must be >= 0")
        if args.on <= 0 or args.off <= 0:
            raise ValueError("ramp on/off durations must be > 0")
    elif args.mode == "pulse_train":
        if args.gain < 0:
            raise ValueError("gain must be >= 0")
        if args.pulse_count < 1:
            raise ValueError("pulse_count must be >= 1")
        if args.on_duration_ms < 1 or args.off_duration_ms < 1:
            raise ValueError("pulse durations must be >= 1 ms")
        if args.end_delay_ms < 0:
            raise ValueError("end_delay_ms must be >= 0")
    elif args.gain < 0:
        raise ValueError("gain must be >= 0")
    return args


def main(argv=None):
    try:
        args = validate_args(parser().parse_args(argv))
    except ValueError as exc:
        parser().error(str(exc))
    return Runner(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
