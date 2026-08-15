"""Single, shared configuration loader for the harness.

Loads ``mapping.yaml`` (per-dataset contract) and ``harness.yaml`` (runtime knobs)
from the project root, or from ``--config-dir``/``SEAL_CONFIG_DIR``. This is the only
place YAML config is read so the rest of the code stays config-agnostic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent


def _config_dir() -> Path:
    override = os.environ.get("SEAL_CONFIG_DIR")
    if override:
        return Path(override)
    return PROJECT_ROOT


@dataclass
class MappedSignal:
    signal_id: str
    source: str
    area: str
    kind: str
    unit: str = ""
    scale: float = 1.0

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MappedSignal":
        return cls(
            signal_id=d["signal_id"],
            source=d["source"],
            area=d.get("area", ""),
            kind=d.get("kind", "raw"),
            unit=d.get("unit", ""),
            scale=float(d.get("scale", 1.0)),
        )


@dataclass
class Pairing:
    signal: str
    setpoint: str
    feedback: str


@dataclass
class Topology:
    assets: dict[str, dict] = field(default_factory=dict)
    connections: list[dict] = field(default_factory=list)


@dataclass
class Command:
    name: str
    target: str
    min: float
    max: float
    safe_min: float
    safe_max: float
    default: float
    unit: str = ""
    energy_cost: float = 0.5
    risk_baseline: float = 0.2
    effect_gain: float = 0.9

    @classmethod
    def from_dict(cls, d: dict) -> "Command":
        return cls(
            name=d["name"],
            target=d["target"],
            min=float(d.get("min", 0.0)),
            max=float(d.get("max", 100.0)),
            safe_min=float(d.get("safe_min", 0.0)),
            safe_max=float(d.get("safe_max", 100.0)),
            default=float(d.get("default", 0.0)),
            unit=d.get("unit", ""),
            energy_cost=float(d.get("energy_cost", 0.5)),
            risk_baseline=float(d.get("risk_baseline", 0.2)),
            effect_gain=float(d.get("effect_gain", 0.9)),
        )


@dataclass
class Mapping:
    def __init__(
        self,
        dataset: str,
        rate_hz: float,
        columns: list[MappedSignal],
        pairing: list[Pairing] | None = None,
        label_columns: list[str] | None = None,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        topology: Topology | None = None,
        commands: list[Command] | None = None,
    ) -> None:
        self.dataset = dataset
        self.rate_hz = rate_hz
        self.columns = columns
        self.pairing = pairing or []
        self.label_columns = label_columns or []
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topology = topology or Topology()
        self.commands = commands or []
        self._by_id = {c.signal_id: c for c in columns}

    @property
    def by_id(self) -> dict[str, MappedSignal]:
        return self._by_id

    def pair_for(self, signal_id: str) -> Pairing | None:
        for p in self.pairing:
            if signal_id in (p.signal, p.setpoint, p.feedback):
                return p
        return None

    def asset_for(self, signal_id: str) -> str | None:
        for name, meta in self.topology.assets.items():
            if signal_id in meta.get("signals", []):
                return name
        return None

    def command_for(self, target_signal: str) -> Command | None:
        for c in self.commands:
            if c.target == target_signal:
                return c
        return None

    def validate(self) -> list[str]:
        """Fail-fast: return a list of unresolved/duplicate tag problems."""
        errors: list[str] = []
        ids = [c.signal_id for c in self.columns]
        if len(ids) != len(set(ids)):
            errors.append("duplicate signal_id in mapping.columns")
        for p in self.pairing:
            for ref in (p.signal, p.setpoint, p.feedback):
                if ref not in self.by_id:
                    errors.append(f"pairing references unknown signal {ref!r}")
        if self.rate_hz <= 0:
            errors.append("rate_hz must be > 0")
        names = [c.name for c in self.commands]
        if len(names) != len(set(names)):
            errors.append("duplicate command name")
        for c in self.commands:
            if c.safe_min > c.safe_max:
                errors.append(f"command {c.name}: safe_min > safe_max")
            if c.safe_min < c.min or c.safe_max > c.max:
                errors.append(f"command {c.name}: safe envelope outside physical range")
        return errors


def load_mapping(path: Path | None = None) -> Mapping:
    cfg = _load_yaml_path(path or (_config_dir() / "mapping.yaml"))
    columns = [MappedSignal.from_dict(c) for c in cfg.get("columns", [])]
    pairing = [Pairing(**p) for p in cfg.get("pairing", [])]
    m = Mapping(
        dataset=cfg.get("dataset", "unknown"),
        rate_hz=float(cfg.get("rate_hz", 1.0)),
        columns=columns,
        pairing=pairing,
        label_columns=list(cfg.get("label_columns", [])),
        broker_host=cfg.get("broker", {}).get("host", "localhost"),
        broker_port=int(cfg.get("broker", {}).get("port", 1883)),
        topology=_parse_topology(cfg.get("topology", {})),
        commands=[Command.from_dict(c) for c in cfg.get("commands", [])],
    )
    errs = m.validate()
    if errs:
        raise ValueError("mapping.yaml invalid: " + "; ".join(errs))
    return m


def _parse_topology(t: dict) -> Topology:
    return Topology(
        assets=dict(t.get("assets", {}) or {}),
        connections=list(t.get("connections", []) or []),
    )


@dataclass
class RunbookConfig:
    jaccard_threshold: float = 0.6
    min_tokens_match: int = 2
    store_path: str = "knowledge/runbooks"


@dataclass
class DiagnosisConfig:
    causal_lags: list[int] = field(default_factory=lambda: [1, 2, 3])
    correlation_min: float = 0.6
    granger_enabled: bool = True
    granger_maxlag: int = 2
    granger_pvalue: float = 0.05
    max_hypotheses: int = 3


@dataclass
class LLMConfig:
    provider: str = "openrouter"
    default_model: str = "anthropic/claude-sonnet-4-20250514"
    budget_per_call_path: int = 3
    fallback: str = "single_pass"


@dataclass
class DebateConfig:
    role_order: list[str] = field(default_factory=lambda: ["proposer", "critic", "arbiter"])
    max_turns: int = 1


@dataclass
class DecideConfig:
    weights: dict = field(default_factory=lambda: {"alpha": 0.5, "beta": 0.15, "gamma": 0.2, "delta": 0.15})
    candidate_count: int = 2


@dataclass
class VerifyConfig:
    window_samples: int = 20
    improved_threshold: float = 0.3
    worsened_threshold: float = 0.1


@dataclass
class IncidentConfig:
    retry_max: int = 3
    ttl_seconds_event: float = 3600.0
    db_path: str = "incident/incidents.db"


@dataclass
class HarnessConfig:
    event_rate_hz: float = 1.0
    adwin_delta: float = 0.002
    ema_span: float = 20.0
    anomaly_gain: float = 1.0
    symptom_taxonomy: list[str] = field(default_factory=list)
    runbook: RunbookConfig = field(default_factory=RunbookConfig)
    diagnosis: DiagnosisConfig = field(default_factory=DiagnosisConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    debate: DebateConfig = field(default_factory=DebateConfig)
    decide: DecideConfig = field(default_factory=DecideConfig)
    verify: VerifyConfig = field(default_factory=VerifyConfig)
    incident: IncidentConfig = field(default_factory=IncidentConfig)

    @classmethod
    def load(cls, path: Path | None = None) -> "HarnessConfig":
        cfg = _load_yaml_path(path or (_config_dir() / "harness.yaml"))
        return cls(
            event_rate_hz=float(cfg.get("event_rate_hz", 1.0)),
            adwin_delta=float(cfg.get("adwin", {}).get("delta", 0.002)),
            ema_span=float(cfg.get("ema", {}).get("span", 20.0)),
            anomaly_gain=float(cfg.get("anomaly_gain", 1.0)),
            symptom_taxonomy=list(cfg.get("symptom_taxonomy", [])),
            runbook=_as(RunbookConfig, cfg.get("runbook", {})),
            diagnosis=_as(DiagnosisConfig, cfg.get("diagnosis", {})),
            llm=_as(LLMConfig, cfg.get("llm", {})),
            debate=_as(DebateConfig, cfg.get("debate", {})),
            decide=_as(DecideConfig, cfg.get("decide", {})),
            verify=_as(VerifyConfig, cfg.get("verify", {})),
            incident=_as(IncidentConfig, cfg.get("incident", {})),
        )


def _as(dc, data: dict):
    """Map a dict onto a dataclass, ignoring unknown keys."""
    import dataclasses

    allowed = {f.name for f in dataclasses.fields(dc)}
    return dc(**{k: v for k, v in data.items() if k in allowed})


def _load_yaml_path(path: Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"config not found: {p}")
    with p.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"config must be a YAML map: {p}")
    return data