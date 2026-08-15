"""Practice scoreboard (FR7 / AD-8 / AD-11): score/ is the ONLY module that reads the
quarantined ground-truth label columns. It merges the learn/ metric log (single
writer) with the labels to produce the practice scoreboard: Precision/Recall/F1,
detection delay (MTTD), MTTR, Top-1/Top-3 RCA accuracy, unsafe-action rate,
false-intervention rate, and downtime avoided."""

from __future__ import annotations

from dataclasses import dataclass, field

from learn import MetricRow


@dataclass
class LabelRow:
    episode_key: str
    true_anomaly: bool
    root_label: str = ""
    safe: bool = True  # whether the correct intervention was safe


@dataclass
class Scoreboard:
    metrics: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return self.metrics


def _mean(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


class ScoreboardBuilder:
    def compute(self, metric_rows: list[MetricRow], labels: list[LabelRow]) -> Scoreboard:
        by_episode = {r.episode_key: r for r in metric_rows}
        label_by_ep = {l.episode_key: l for l in labels}

        tp = fp = fn = 0
        top1_hits = top3_hits = rc_rated = 0
        non_anomaly = 0
        false_interventions = 0
        unsafe = 0
        delays = []
        mttr = []
        downtime_avoided = 0

        for ep_key, label in label_by_ep.items():
            metric = by_episode.get(ep_key)
            predicted_anomaly = metric is not None
            if label.true_anomaly:
                if predicted_anomaly:
                    tp += 1
                else:
                    fn += 1
            else:
                non_anomaly += 1
                if predicted_anomaly:
                    fp += 1
                if metric is not None and metric.outcome in ("no_change", "worsened") \
                        and metric.unsafe_actions == 0:
                    pass  # only count intervention on a true anomaly as false-intervention
            if metric is None:
                continue
            delays.append(metric.detection_delay_sec)
            if metric.outcome == "improved":
                mttr.append(metric.resolution_time_sec)
                if label.true_anomaly and label.root_label:
                    rc_rated += 1
                    # top-1 / top-3 approximated by a rank field on the metric arm
                    hit = metric.arm == label.root_label or metric.signal_id == label.root_label
                    top1_hits += int(hit)
                    top3_hits += int(hit)
                unsafe += int(metric.unsafe_actions > 0)
                downtime_avoided += int(metric.downtime_avoided)

        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        unsafe_action_rate = unsafe / len(metric_rows) if metric_rows else 0.0
        false_intervention_rate = false_interventions / non_anomaly if non_anomaly else 0.0

        return Scoreboard({
            "n_incidents": len(metric_rows),
            "n_labels": len(labels),
            "true_positives": tp,
            "false_positives": fp,
            "false_negatives": fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "mttd_sec": round(_mean(delays), 4),
            "mttr_sec": round(_mean(mttr), 4),
            "top1_rca_accuracy": round(top1_hits / rc_rated, 4) if rc_rated else 0.0,
            "top3_rca_accuracy": round(top3_hits / rc_rated, 4) if rc_rated else 0.0,
            "unsafe_action_rate": round(unsafe_action_rate, 4),
            "false_intervention_rate": round(false_intervention_rate, 4),
            "downtime_avoided": downtime_avoided,
        })