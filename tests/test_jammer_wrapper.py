import csv
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


wrapper = load_module(
    "jammer_wrapper", ROOT / "ansible/scripts/jammer-wrapper.py"
)


class FakeResponse:
    def raise_for_status(self):
        pass

    def json(self):
        return {"ok": True}


def test_legacy_arguments_default_to_ramp():
    args = wrapper.validate_args(
        wrapper.parser().parse_args(
            [
                "--start_gain",
                "60",
                "--end_gain",
                "90",
                "--steps",
                "4",
                "--on",
                "10",
                "--off",
                "20",
            ]
        )
    )
    assert args.mode == "ramp"
    assert args.start_gain == 60


def test_pulse_train_emits_finite_transitions_and_state(tmp_path):
    events = []

    def fake_get(url, timeout):
        events.append(url.removeprefix("http://jammer"))
        return FakeResponse()

    with patch.object(wrapper.requests, "get", side_effect=fake_get):
        state_path = tmp_path / "state.json"
        log_path = tmp_path / "transitions.csv"
        args = wrapper.validate_args(
            wrapper.parser().parse_args(
                [
                    "--mode",
                    "pulse_train",
                    "--run-id",
                    "test pulse",
                    "--api-url",
                    "http://jammer",
                    "--state-file",
                    str(state_path),
                    "--log-file",
                    str(log_path),
                    "--gain",
                    "60",
                    "--pulse-count",
                    "2",
                    "--start-delay-ms",
                    "1",
                    "--on-duration-ms",
                    "2",
                    "--off-duration-ms",
                    "2",
                    "--end-delay-ms",
                    "1",
                ]
            )
        )

        assert wrapper.Runner(args).run() == 0

        assert events == [
            "/off",
            "/on?gain=60.0",
            "/off",
            "/on?gain=60.0",
            "/off",
            "/off",
        ]
        assert json.loads(state_path.read_text())["status"] == "completed"
        rows = list(csv.DictReader(log_path.open()))
        assert [row["action"] for row in rows] == [
            "off",
            "on",
            "off",
            "on",
            "off",
            "off",
        ]
        assert all(row["success"] == "True" for row in rows)


def test_pulse_validation_rejects_zero_duration():
    args = wrapper.parser().parse_args(
        ["--mode", "pulse_train", "--gain", "60", "--on-duration-ms", "0"]
    )
    try:
        wrapper.validate_args(args)
    except ValueError as exc:
        assert "durations" in str(exc)
    else:
        raise AssertionError("zero pulse duration was accepted")


def test_transition_retries_once_and_logs_warning(tmp_path):
    state = wrapper.StateFile(tmp_path / "state.json", "pulse_train")
    transition_log = wrapper.TransitionLog(
        tmp_path / "transitions.csv", "pulse_train", state
    )
    controller = wrapper.JammerController("http://jammer", transition_log)
    calls = 0

    def failing_get(_url, timeout):
        nonlocal calls
        calls += 1
        raise RuntimeError("offline")

    with patch.object(wrapper.requests, "get", side_effect=failing_get):
        _, success = controller.transition("on", 60)
    transition_log.close()

    row = next(csv.DictReader((tmp_path / "transitions.csv").open()))
    assert calls == 2
    assert success is False
    assert row["attempts"] == "2"
    assert "offline" in row["warning"]
    assert state.warnings == 1
