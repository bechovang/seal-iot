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
        self.fsm = IncidentFSM(self.incident_store, harness.incident.retry_max)
        from history import HistoryBuffer
        self.history = history or HistoryBuffer(":memory:")
        self.verify = OutcomeClassifier(self.history, harness.verify)

    def run(self, diagnosis: Diagnosis, baseline: float = 0.0,
            after_epoch_ms: int | None = None) -> dict:
        """Full guarded-act loop for one diagnosis; returns the incident state + outcome."""
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
        inc = self.fsm.verify_outcome(incident_id, outcome.classification)
        return {
            "incident_id": incident_id,
            "executed": True,
            "decision": decision,
            "outcome": outcome.to_dict(),
            "state": inc.state,
        }