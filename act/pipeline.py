"""Guarded-action orchestration (AD-9): given a diagnosis, generate candidates via
DECIDE, gate the winner through the deterministic safety shield, execute the approved
command via the sole executor, and hand the outcome classification to the incident FSM
so only 'improved' resolves and no remediation completes silently."""

from __future__ import annotations

from config import Command, HarnessConfig, Mapping
from diagnose import Diagnosis
from decide import Decider
from incident import IncidentFSM, IncidentStore
from plant_model import PlantModel
from verify import OutcomeClassifier


class GuardedActionPipeline:
    def __init__(
        self,
        harness: HarnessConfig,
        mapping: Mapping,
        bus,
        history=None,
        incident_store: IncidentStore | None = None,
        model: PlantModel | None = None,
        runbook_store=None,
        metric_store=None,
    ) -> None:
        from act import ActExecutor
        from decide import Decider

        self.harness = harness
        self.mapping = mapping
        self.bus = bus
        self.model = model or PlantModel()
        self.decider = Decider(mapping, harness.decide, self.model,
                               static=harness.variant.decide_static(),
                               red_team=harness.variant.red_team)
        self.executor = ActExecutor(bus)
        self.incident_store = incident_store or IncidentStore(harness.incident.db_path,
                                                              harness.incident.retry_max,
                                                              harness.incident.ttl_seconds_event)
        self.metric_store = metric_store
        self.runbook_store = runbook_store
        self._last_outcome = ""
        self._last_runbook = None
        self.fsm = IncidentFSM(self.incident_store, harness.incident.retry_max,
                               distill_hook=self._distill)
        from history import HistoryBuffer
        self.history = history or HistoryBuffer(":memory:")
        self.verify = OutcomeClassifier(self.history, harness.verify)

    def _publish(self, stage: str, event: str, payload: dict, ts: str = "",
                 episode_key: str = "") -> None:
        """Best-effort additive publish on ``ops/<stage>``. Guarded so a consumer-only
        loop (or a bus without publish_event) never breaks the pipeline; the demo
        feed for the control-room dashboard is purely additive."""
        bus = getattr(self, "bus", None)
        if bus is None or not hasattr(bus, "publish_event"):
            return
        try:
            bus.publish_event(stage, event, "act", ts, payload,
                              episode_key=episode_key or None)
        except Exception:
            pass

    def _fsm_trail(self, incident_id: str, episode_key: str, event: str,
                   from_state: str, to_state: str, retries: int = 0,
                   ts: str = "") -> None:
        """Emit an ops/incident FSM-trail event for every transition so the control
        room can render the stepper (additive; never in the stable contracts)."""
        self._publish("incident", event, {
            "incident_id": incident_id, "episode_key": episode_key,
            "event": event, "from_state": from_state, "to_state": to_state,
            "retries": retries, "ts": ts,
        }, ts=ts, episode_key=episode_key)

    def _distill(self, incident) -> None:
        """AD-3: distil resolved incidents into the runbook wiki; AD-11: record the
        per-incident metric row. Both fire inside the resolve transaction."""
        self._last_runbook = None
        if self.runbook_store is not None and self._diag is not None:
            self._last_runbook = self.runbook_store.record_resolution(
                self._diag.symptom_tokens or ["recur_symptom"],
                self._diag.root_cause or "unknown",
                self._diag.action_hint or "recalibrate",
            )
        metric = None
        if self.metric_store is not None:
            from learn import MetricRow

            metric = MetricRow(
                incident_id=incident.incident_id,
                episode_key=self._diag.episode_key if self._diag else incident.episode_key,
                detection_delay_sec=self._detect_delay,
                rca_latency_sec=self._rca_latency,
                resolution_time_sec=self._resolution_time,
                outcome=self._last_outcome or "improved",
                arm=self._arm,
                signal_id=self._diag.signal_id if self._diag else "",
            )
            self.metric_store.record(metric)
        # AD-11 additive: surface the distillation on the bus for the learn panel.
        self._publish("learn", "learn", {
            "incident_id": incident.incident_id,
            "episode_key": self._diag.episode_key if self._diag else incident.episode_key,
            "runbook": self._last_runbook.to_dict() if self._last_runbook else None,
            "metric": metric.to_dict() if metric else None,
            "ts": getattr(self._diag, "ts", "") or "",
        }, ts=getattr(self._diag, "ts", "") or "",
           episode_key=self._diag.episode_key if self._diag else incident.episode_key)

    def run(self, diagnosis: Diagnosis, baseline: float = 0.0,
            after_epoch_ms: int | None = None,
            detection_delay_sec: float = 0.0, rca_latency_sec: float = 0.0,
            resolution_time_sec: float = 0.0, arm: str = "") -> dict:
        """Full guarded-act loop for one diagnosis; returns the incident state + outcome."""
        self._diag = diagnosis
        self._detect_delay = detection_delay_sec
        self._rca_latency = rca_latency_sec
        self._resolution_time = resolution_time_sec
        self._arm = arm
        incident_id = self.fsm.create(diagnosis.episode_key)
        self.fsm.transition(incident_id, "start_detected", ts=diagnosis.ts)
        self.fsm.transition(incident_id, "diagnose", ts=diagnosis.ts)
        self._fsm_trail(incident_id, diagnosis.episode_key, "diagnose",
                        "DETECTED", "DIAGNOSING", ts=diagnosis.ts)

        cmd = self.mapping.command_for(diagnosis.signal_id)
        if cmd is None:
            pair = self.mapping.pair_for(diagnosis.signal_id)
            if pair is not None:
                cmd = self.mapping.command_for(pair.setpoint) or self.mapping.command_for(pair.feedback)
        decision = self.decider.decide(diagnosis.to_dict(), cmd, baseline)
        self._publish("decide", "decide", {
            "incident_id": incident_id,
            "episode_key": diagnosis.episode_key,
            "winner": decision["winner"],
            "comparison": decision["comparison"],
            "noop_row": decision["noop_row"],
            "objective": decision["objective"],
            "ts": diagnosis.ts,
        }, ts=diagnosis.ts, episode_key=diagnosis.episode_key)
        self.fsm.transition(incident_id, "plan", ts=diagnosis.ts)
        self._fsm_trail(incident_id, diagnosis.episode_key, "plan",
                        "DIAGNOSING", "PLANNING", ts=diagnosis.ts)

        winner = decision["winner"]
        action, target = winner.get("action"), winner.get("target", 0.0)
        from act import Action

        action_obj = Action(cmd if winner.get("action") != "do_nothing" else None, target)
        executed = self.executor.execute(action_obj, diagnosis.ts, incident_id)
        if executed["executed"]:
            self.fsm.transition(incident_id, "act", ts=diagnosis.ts)
            self._fsm_trail(incident_id, diagnosis.episode_key, "act",
                            "PLANNING", "ACTING", ts=diagnosis.ts)
        else:
            self.fsm.transition(incident_id, "escalate", ts=diagnosis.ts)
            self._fsm_trail(incident_id, diagnosis.episode_key, "escalate",
                            "PLANNING", "ESCALATED", ts=diagnosis.ts)

        if not executed["executed"]:
            return {"incident_id": incident_id, "executed": False,
                    "reason": executed["reason"], "decision": decision}

        self.fsm.transition(incident_id, "verify", ts=diagnosis.ts)
        self._fsm_trail(incident_id, diagnosis.episode_key, "verify",
                        "ACTING", "VERIFYING", ts=diagnosis.ts)
        outcome = self.verify.classify(diagnosis.signal_id, baseline,
                                       after_epoch_ms=after_epoch_ms,
                                       expected_effect=winner.get("predicted"))
        self._last_outcome = outcome.classification
        self._publish("verify", "verify", {
            "incident_id": incident_id,
            "episode_key": diagnosis.episode_key,
            "classification": outcome.classification,
            "baseline": outcome.baseline,
            "post": outcome.post,
            "relative_change": outcome.relative_change,
            "expected_effect": outcome.expected_effect,
            "ts": diagnosis.ts,
        }, ts=diagnosis.ts, episode_key=diagnosis.episode_key)
        inc = self.fsm.verify_outcome(incident_id, outcome.classification, ts=diagnosis.ts)
        self._fsm_trail(incident_id, diagnosis.episode_key,
                        "resolve" if inc.state == "RESOLVED" else
                        ("retry" if inc.state == "DIAGNOSING" else "escalate"),
                        "VERIFYING", inc.state, inc.retries, ts=diagnosis.ts)
        return {
            "incident_id": incident_id,
            "executed": True,
            "decision": decision,
            "outcome": outcome.to_dict(),
            "state": inc.state,
        }