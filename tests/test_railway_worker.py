from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

from ai_advokat_parser import railway_worker
from ai_advokat_parser.sot.model import (
    PHASE_ABORTED,
    PHASE_COMPLETED,
    PHASE_PAUSED,
    PHASE_RATE_LIMITED,
)


COMMAND = "--out /tmp/sot --delay 0 sot-scan --scan-id all-sot-v1"


def state(phase: str, note: str | None = None):
    return SimpleNamespace(phase=phase, rate_limit_note=note)


class RailwayWorkerCompatibilityTests(unittest.TestCase):
    def test_disabled_supervisor_keeps_one_shot_behavior(self) -> None:
        with (
            mock.patch.dict(
                railway_worker.os.environ,
                {"AI_ADVOCAT_COMMAND": "sot-status", railway_worker.AUTO_RESUME_ENV: "false"},
                clear=True,
            ),
            mock.patch.object(railway_worker, "cli_main") as cli_main,
            mock.patch.object(railway_worker, "sleep_forever") as sleep_forever,
        ):
            railway_worker.main()

        cli_main.assert_called_once_with(["sot-status"])
        sleep_forever.assert_called_once_with()

    def test_enabled_supervisor_rejects_non_scan_before_running(self) -> None:
        runner = mock.Mock()
        with self.assertRaises(railway_worker.AutoResumeConfigError):
            railway_worker.supervise_sot_scan("sot-status", env={}, runner=runner)
        runner.assert_not_called()


class RailwaySotSupervisorTests(unittest.TestCase):
    def run_sequence(self, states, env=None):
        pending = list(states)
        runner = mock.Mock()
        sleeper = mock.Mock()

        def load(_out, _scan_id, _env):
            return pending.pop(0)

        result = railway_worker.supervise_sot_scan(
            COMMAND,
            env=env or {},
            runner=runner,
            state_loader=load,
            sleeper=sleeper,
        )
        return result, runner, sleeper

    def test_completed_scan_is_not_rerun(self) -> None:
        result, runner, sleeper = self.run_sequence([state(PHASE_COMPLETED)])
        self.assertEqual(result.phase, PHASE_COMPLETED)
        runner.assert_called_once()
        sleeper.assert_not_called()

    def test_rate_limit_waits_for_longest_header_plus_safety(self) -> None:
        result, runner, sleeper = self.run_sequence(
            [
                state(PHASE_RATE_LIMITED, "rate limit: retry-after=100s, reset-in=120s, remaining=0"),
                state(PHASE_COMPLETED),
            ],
            {railway_worker.AUTO_RESUME_SAFETY_ENV: "15"},
        )
        self.assertEqual(result.phase, PHASE_COMPLETED)
        self.assertEqual(runner.call_count, 2)
        sleeper.assert_called_once_with(135.0)

    def test_rate_limit_without_headers_uses_bounded_fallback(self) -> None:
        _, _, sleeper = self.run_sequence(
            [state(PHASE_RATE_LIMITED, "remaining=0"), state(PHASE_COMPLETED)],
            {
                railway_worker.AUTO_RESUME_FALLBACK_ENV: "600",
                railway_worker.AUTO_RESUME_SAFETY_ENV: "10",
            },
        )
        sleeper.assert_called_once_with(610.0)

    def test_rate_limit_wait_is_capped_before_retrying_the_429(self) -> None:
        _, _, sleeper = self.run_sequence(
            [state(PHASE_RATE_LIMITED, "reset-in=10000s, remaining=0"), state(PHASE_COMPLETED)],
            {
                railway_worker.AUTO_RESUME_MAX_WAIT_ENV: "600",
                railway_worker.AUTO_RESUME_SAFETY_ENV: "30",
                railway_worker.AUTO_RESUME_FALLBACK_ENV: "600",
            },
        )
        sleeper.assert_called_once_with(600.0)

    def test_paused_scan_retries_after_recoverable_backoff(self) -> None:
        _, runner, sleeper = self.run_sequence(
            [state(PHASE_PAUSED), state(PHASE_COMPLETED)],
            {railway_worker.AUTO_RESUME_PAUSED_ENV: "45"},
        )
        self.assertEqual(runner.call_count, 2)
        sleeper.assert_called_once_with(45.0)

    def test_aborted_scan_stops_without_sleeping_or_rerunning(self) -> None:
        runner = mock.Mock()
        sleeper = mock.Mock()
        with self.assertRaisesRegex(railway_worker.AutoResumeFatalError, "aborted"):
            railway_worker.supervise_sot_scan(
                COMMAND,
                env={},
                runner=runner,
                state_loader=lambda *_: state(PHASE_ABORTED),
                sleeper=sleeper,
            )
        runner.assert_called_once()
        sleeper.assert_not_called()

    def test_command_failure_is_fatal_and_not_retried(self) -> None:
        runner = mock.Mock(side_effect=RuntimeError("auth rejected"))
        sleeper = mock.Mock()
        with self.assertRaisesRegex(railway_worker.AutoResumeFatalError, "auth rejected"):
            railway_worker.supervise_sot_scan(
                COMMAND,
                env={},
                runner=runner,
                state_loader=mock.Mock(),
                sleeper=sleeper,
            )
        runner.assert_called_once()
        sleeper.assert_not_called()


if __name__ == "__main__":
    unittest.main()
