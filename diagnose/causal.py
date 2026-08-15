"""Causal graph builder (AD-5): heuristic edge weights, never certified causality.

RCA reasons over a graph whose edges are weights from windowed lag-correlation and
(optionally) Granger causality tests over the history buffer. Direction is refined
by Granger when enabled; otherwise the edge is an undirected association. Every edge
is marked ``heuristic: true`` so downstream never mistakes it for certified causation.
"""

from __future__ import annotations

import math

import networkx as nx
import numpy as np

from config import DiagnosisConfig
from history import HistoryBuffer


def _lag_correlation(a: list[float], b: list[float], lag: int, sign: int) -> float:
    """Pearson correlation between a[t] and b[t + sign*lag], aligned in time."""
    n = len(a)
    if n < 2 * abs(lag) + 4:
        return 0.0
    if sign > 0:
        x = np.asarray(a[lag:], dtype=float)
        y = np.asarray(b[: n - lag], dtype=float)
    else:
        x = np.asarray(a[: n - lag], dtype=float)
        y = np.asarray(b[lag:], dtype=float)
    if len(x) < 3:
        return 0.0
    sx, sy = float(x.std()), float(y.std())
    if sx <= 1e-9 or sy <= 1e-9:
        return 0.0
    return float(np.corrcoef(x, y)[0, 1])


class CausalGraphBuilder:
    def __init__(self, config: DiagnosisConfig | None = None, use_granger: bool = True) -> None:
        self.config = config or DiagnosisConfig()
        self.use_granger = use_granger

    def build(self, signal_ids: list[str], history: HistoryBuffer) -> nx.DiGraph:
        series = history.window_series(signal_ids, limit=800)
        graph = nx.DiGraph()
        for sid in signal_ids:
            graph.add_node(sid)
        ids = [s for s in signal_ids if s in series and len(series[s]) >= 4]
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                a, b = ids[i], ids[j]
                a_vals = [v for _, v in series[a]]
                b_vals = [v for _, v in series[b]]
                w_ab, w_ba = 0.0, 0.0
                for lag in self.config.causal_lags:
                    w_ab = max(w_ab, abs(_lag_correlation(a_vals, b_vals, lag, 1)))
                    w_ba = max(w_ba, abs(_lag_correlation(b_vals, a_vals, lag, 1)))
                if max(w_ab, w_ba) < self.config.correlation_min:
                    continue
                # direction: Granger refines when enabled and significant
                fwd = rev = False
                if self.use_granger:
                    fwd, _ = self._granger(a_vals, b_vals)
                    rev, _ = self._granger(b_vals, a_vals)
                    if fwd:
                        w_ba = 0.0
                    elif rev:
                        w_ab = 0.0
                if w_ab >= self.config.correlation_min:
                    graph.add_edge(
                        a, b, weight=w_ab,
                        method="granger" if fwd else "lagkor", heuristic=True,
                    )
                if w_ba >= self.config.correlation_min:
                    graph.add_edge(
                        b, a, weight=w_ba,
                        method="granger" if rev else "lagkor", heuristic=True,
                    )
        return graph

    def _granger(self, x: list[float], y: list[float]) -> tuple[bool, float]:
        """True if x Granger-causes y at significance. Gracefully empty when statsmodels
        is unavailable or the series is too short (falls back to association)."""
        if not self.config.granger_enabled or len(x) < 20 or len(y) < 20:
            return False, 1.0
        try:
            import statsmodels.api as sm
        except Exception:
            return False, 1.0
        try:
            data = np.column_stack([y[: min(len(y), len(x))], x[: min(len(y), len(x))]])
            res = sm.tsa.stattools.grangercausalitytests(data, maxlag=self.config.granger_maxlag, verbose=False)
            best = min(res[lag][0]["ssr_ftest"][1] for lag in res)
            return best < self.config.granger_pvalue, float(best)
        except Exception:
            return False, 1.0

    @staticmethod
    def describe(graph: nx.DiGraph) -> list[dict]:
        """Heuristic edge summary for presentation (never 'causal' language)."""
        return [
            {"source": u, "target": v, "weight": round(d["weight"], 3), "heuristic": True}
            for u, v, d in graph.edges(data=True)
        ]