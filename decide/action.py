"""DECIDE (AD-6): generate action candidates + the mandatory do(empty-set) no-action,
simulate each via the shared plant model, and score with a visible objective function:

    score = alpha*safety + beta*energy - gamma*downtime - delta*risk

Each term is normalized to [0,1]; weights are pinned in harness.yaml so winners are
reproducible. The winner ships with the full comparison including the do(empty-set)
counterfactual row, making the artifact a defensible 'cost of inaction' explanation.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from config import Command, DecideConfig, Mapping
from plant_model import PlantModel


@dataclass
class ActionCandidate:
    command: Command | None
    target: float
    from_llm: bool = False
    is_noop: bool = False
    name: str = ""

    def __post_init__(self) -> None:
        base = "do_nothing" if self.is_noop else (self.command.name if self.command else "unknown")
        self.name = base


@dataclass
class CandidateScore:
    action: ActionCandidate
    predicted: float
    safety: float
    energy: float
    downtime: float
    risk: float
    weighted: float

    def to_dict(self) -> dict:
        return {
            "action": self.action.name,
            "target": self.action.target,
            "noop": self.action.is_noop,
            "predicted": round(self.predicted, 4),
            "safety": round(self.safety, 4),
            "energy": round(self.energy, 4),
            "downtime": round(self.downtime, 4),
            "risk": round(self.risk, 4),
            "score": round(self.weighted, 4),
        }


def _clamp(x: float) -> float:
    return max(0.0, min(1.0, x))


class CandidateGenerator:
    """Deterministic candidates (LLM may propose more; control values never come from
    the LLM). Always >= candidate_count real candidates plus do(empty-set)."""

    def __init__(self, cfg: DecideConfig) -> None:
        self.cfg = cfg

    def generate(self, mapping: Mapping, command: Command | None,
                 red_team: bool = False) -> list[ActionCandidate]:
        cands: list[ActionCandidate] = [ActionCandidate(None, 0.0, is_noop=True)]
        if command is None:
            return cands
        default = command.default
        span = max(command.safe_max - command.safe_min, 1e-9)
        reductions = [default - 0.2 * span, default - 0.4 * span]
        for i in range(max(1, self.cfg.candidate_count)):
            target = _clamp_into(command, reductions[i % len(reductions)])
            cands.append(ActionCandidate(command, target, from_llm=False))
        # ensure at least candidate_count real + the noop
        while len([c for c in cands if not c.is_noop]) < self.cfg.candidate_count:
            target = default - 0.5 * span * (len(cands))
            cands.append(ActionCandidate(command, _clamp_into(command, target)))
        if red_team:
            # 5.5: inject known-BAD candidates (outside the safe envelope) so the
            # shield's block rate is measured against real risk, not vacuously zero.
            bad_high = ActionCandidate(command, command.max * 1.5, from_llm=True)
            bad_low = ActionCandidate(command, -0.5, from_llm=True)
            cands.extend([bad_high, bad_low])
        return cands


def _clamp_into(command: Command, target: float) -> float:
    return max(command.safe_min, min(command.safe_max, target))


class ObjectiveScorer:
    def __init__(self, model: PlantModel, cfg: DecideConfig, mapping: Mapping,
                 static: bool = False) -> None:
        self.model = model
        self.cfg = cfg
        self.mapping = mapping
        self.static = static  # 5.4: what-if sim -> static command-registry priors

    def score(self, candidate: ActionCandidate, baseline: float) -> CandidateScore:
        w = self.cfg.weights
        alpha = float(w.get("alpha", 0.5))
        beta = float(w.get("beta", 0.15))
        gamma = float(w.get("gamma", 0.2))
        delta = float(w.get("delta", 0.15))
        # normalize weights so the objective is comparable even if weights are tuned
        total = alpha + beta + gamma + delta
        if total > 0:
            alpha, beta, gamma, delta = alpha / total, beta / total, gamma / total, delta / total

        if candidate.is_noop or candidate.command is None:
            predicted = baseline
            safety = _clamp(1.0 - _risk(candidate, baseline))
            energy = _clamp(1.0 - candidate.command.energy_cost) if candidate.command else 0.0
            downtime = 1.0  # doing nothing leaves the breach in place -> max downtime
            risk = 0.8  # unhandled anomaly carries high residual risk
        elif self.static:
            # AD-13 degraded: no what-if simulation. Score from static command-registry
            # priors only (safe envelope + default + target).
            cmd = candidate.command
            predicted = _clamp_into(cmd, candidate.target)
            mid = 0.5 * (cmd.safe_min + cmd.safe_max)
            span = max(cmd.safe_max - cmd.safe_min, 1e-9)
            safety = _clamp(1.0 - abs(candidate.target - mid) / (span + 1e-9))
            energy = _clamp(0.5 + 0.5 * (cmd.max - candidate.target) / max(cmd.max - cmd.safe_min, 1e-9))
            downtime = _clamp(abs(candidate.target - cmd.default) / max(cmd.max - cmd.min, 1e-9))
            risk = _clamp(_risk(candidate, baseline))
        else:
            cmd = candidate.command
            sim = self.model.predict(cmd, candidate.target, baseline)
            predicted = sim["predicted"]
            safety = _clamp(_safety_term(cmd, candidate.target, predicted))
            energy = _clamp(_energy_term(cmd, candidate.target, predicted))
            downtime = _clamp(abs(predicted - cmd.default) / max(cmd.max - cmd.min, 1e-9))
            risk = _clamp(_risk(candidate, baseline))

        weighted = alpha * safety + beta * energy - gamma * downtime - delta * risk
        return CandidateScore(
            action=candidate,
            predicted=predicted,
            safety=safety,
            energy=energy,
            downtime=downtime,
            risk=risk,
            weighted=weighted,
        )


def _risk(candidate: ActionCandidate, baseline: float) -> float:
    cmd = candidate.command
    if cmd is None:
        return 0.8
    margin_from_boundary = min(candidate.target - cmd.safe_min, cmd.safe_max - candidate.target)
    envelope_span = max(cmd.safe_max - cmd.safe_min, 1e-9)
    geometric = _clamp(1.0 - margin_from_boundary / envelope_span)
    return _clamp(cmd.risk_baseline * 0.5 + geometric * 0.5)


def _safety_term(cmd: Command, target: float, predicted: float) -> float:
    # closer to the safe-envelope midpoint -> safer; hitting a boundary -> unsafe
    mid = 0.5 * (cmd.safe_min + cmd.safe_max)
    span = max(cmd.safe_max - cmd.safe_min, 1e-9)
    dist = abs(target - mid) / (span)
    ge = abs(predicted - mid) / (span + 1e-9)
    return 1.0 - 0.6 * dist - 0.4 * ge


def _energy_term(cmd: Command, target: float, predicted: float) -> float:
    # reducing the setpoint / flow below max saves energy; scaled to envelope
    span = max(cmd.max - cmd.safe_min, 1e-9)
    target_eff = (cmd.max - target) / span
    pred_eff = (cmd.max - predicted) / span
    return 0.5 * target_eff + 0.5 * pred_eff


class Decider:
    """Orchestrates DECIDE: generate -> simulate -> score -> argmax with the full table."""

    def __init__(
        self, mapping: Mapping, cfg: DecideConfig,
        model: PlantModel | None = None,
        static: bool = False,
        red_team: bool = False,
    ) -> None:
        self.mapping = mapping
        self.cfg = cfg
        self.model = model or PlantModel()
        self.static = static
        self.red_team = red_team
        self.gen = CandidateGenerator(cfg)
        self.scorer = ObjectiveScorer(self.model, cfg, mapping, static=static)

    def decide(
        self, diagnosis: dict, command: Command | None, baseline: float,
    ) -> dict:
        candidates = self.gen.generate(self.mapping, command, red_team=self.red_team)
        scored = [self.scorer.score(c, baseline) for c in candidates]
        scored.sort(key=lambda s: s.weighted, reverse=True)
        winner = scored[0]
        comparison = [s.to_dict() for s in scored]
        return {
            "winner": winner.to_dict(),
            "comparison": comparison,
            "noop_row": next(s.to_dict() for s in scored if s.action.is_noop),
            "objective": {"alpha": 0.5, "beta": 0.15, "gamma": 0.2, "delta": 0.15},
        }