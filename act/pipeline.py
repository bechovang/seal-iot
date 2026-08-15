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
        self.decider = Decider(mapping, harness.decide, self.model)
        self.executor = ActExecutor(bus)
        self.incident_store = incident_store or IncidentStore(harness.incident.db_path,
                                                              harness.incident.retry_max,
                                                              harness.incident.ttl_seconds_event)
        self.metric_store = metric_store
        self.runbook_store = runbook_store
        self._last_outcome = ""
        self.fsm = IncidentFSM(self.incident_store, harness.incident.retry_max,
                               distill_hook=self._distill)
        from history import HistoryBuffer
        self.history = history or HistoryBuffer(":memory:")
        self.verify = OutcomeClassifier(self.history, harness.verify)

    def _distill(self, incident) -> None:
        """AD-3: distil resolved incidents into the runbook wiki; AD-11: record the
        per-incident metric row. Both fire inside the resolve transaction."""
        if self.runbook_store is not None and self._diag is not None:
            self.runbook_store.record_resolution(
                self._diag.symptom_tokens or ["recur_symptom"],
                self._diag.root_cause or "unknown",
                self._diag.action_hint or "recalibrate",
            )
        if self.metric_store is not None:
            from learn import MetricRow

            self.metric_store.record(MetricRow(
                incident_id=incident.incident_id,
                episode_key=self._diag.episode_key if self._diag else incident.episode_key,
                detection_delay_sec=self._detect_delay,
                rca_latency_sec=self._rca_latency,
                resolution_time_sec=self._resolution_time,
                outcome=self._last_outcome or "improved",
                arm=self._arm,
                signal_id=self._diag.signal_id if self._diag else "",
            ))

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
        self.fsm.transition(incident_id, "diagnose")

        cmd = self.mapping.command_for(diagnosis.signal_id)
        if cmd is None:
            pair = self.mapping.pair_for(diagnosis.signal_id)
            if pair is not None:
                cmd = self.mapping.command_for(pair.setpoint) or self.mapping.command_for(pair.feedback)
        decision = self.decider.decide(diagnosis.to_dict(), cmd, baseline)
        self.fsm.transition(incident_id, "plan")

        winner = decision["winner"]
        action, target = winner.get("action"), winner.get("target", 0.0)
        from act import Action

        action_obj = Action(cmd if winner.get("action") != "do_nothing" else None, target)
        executed = self.executor.execute(action_obj, diagnosis.ts, incident_id)
        self.fsm.transition(incident_id, "act" if executed["executed"] else "escalate")

        if not executed["executed"]:
            return {"incident_id": incident_id, "executed": False,
                    "reason": executed["reason"], "decision": decision}

        self.fsm.transition(incident_id, "verify")
        outcome = self.verify.classify(diagnosis.signal_id, baseline,
                                       after_epoch_ms=after_epoch_ms,
                                       expected_effect=winner.get("predicted"))
        self._last_outcome = outcome.classification
        inc = self.fsm.verify_outcome(incident_id, outcome.classification)
        return {
            "incident_id": incident_id,
            "executed": True,
            "decision": decision,
            "outcome": outcome.to_dict(),
            "state": inc.state,
        }