"""tiny-bucket-quota v0.1.0 — hierarchical token-bucket quota allocator.

Why hierarchical?
-----------------
A flat per-key token bucket works for one dimension (e.g. one API key has
1000 RPM). Real workloads are **nested**:

    org:acme (5000 RPM)
    ├── team:backend (2000 RPM)
    │   ├── user:42 (200 RPM)
    │   └── user:43 (200 RPM)
    └── team:frontend (3000 RPM)
        ├── user:alice (500 RPM)
        └── user:bob   (500 RPM)

When user:42 spends 200 RPM, the team:backend bucket must drop to 1800
RPM, and org:acme to 4800 RPM. That's the *hierarchical* invariant.

A flat limiter on user:42 alone would let the team burn through 4000 RPM
total (200 × 20) — twice the team budget.

This library implements exactly that: a tree of token buckets that share
state through a parent/child relationship, with refill propagated
automatically (each bucket refills independently; parents consume when
their children consume).

Public API
----------
- :class:`QuotaTree` — the tree.
  - ``add(parent, child, *, limit, window_seconds)`` — register a node.
  - ``try_acquire(path, *, cost=1, now=None)`` — walk the path; all
    ancestors must have capacity. Returns :class:`QuotaDecision`.
  - ``peek(path, *, now=None)`` — non-mutating check.
  - ``reset(path=None)`` — drop one subtree or all.
  - ``stats()`` — flat counters.
- :class:`QuotaDecision` — `allowed`, `blocked_at` (the path that hit
  zero), `remaining_by_path`.

Use cases
--------
- **AI provider budgets**: per-org → per-team → per-agent budgets that
  auto-balance (the popular "shared pool" pattern in 2026 Q4).
- **Multi-tenant SaaS**: roll per-customer quotas up to per-account.
- **Rate-limiting per cost center**: org → team → user, with the boss
  able to see (and reset) any level.

Example
-------
::

    tree = QuotaTree()
    tree.add(parent=None, child="org:acme", limit=5000, window_seconds=60)
    tree.add(parent="org:acme", child="team:backend", limit=2000, window_seconds=60)
    tree.add(parent="team:backend", child="user:42", limit=200, window_seconds=60)
    d = tree.try_acquire(["org:acme", "team:backend", "user:42"])
    if not d.allowed:
        raise QuotaExceeded(d.blocked_at)
    call_api()
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple


__version__ = "0.1.0"


class QuotaExceeded(Exception):
    """Raised by helpers that wrap :meth:`QuotaTree.try_acquire`."""

    def __init__(self, blocked_at: str, decision: "QuotaDecision") -> None:
        super().__init__(f"quota exhausted at {blocked_at!r}")
        self.blocked_at = blocked_at
        self.decision = decision


@dataclass(frozen=True)
class QuotaDecision:
    """Result of a hierarchical try_acquire.

    Attributes
    ----------
    allowed : bool
    blocked_at : Optional[str]
        Path of the first node that had zero remaining. ``None`` if allowed.
    remaining_by_path : Dict[str, int]
        Remaining capacity at every level along the walked path.
    cost : int
        Cost that was attempted.
    """

    allowed: bool
    blocked_at: Optional[str]
    remaining_by_path: Dict[str, int]
    cost: int

    def as_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass
class _Bucket:
    """Token bucket for one node."""

    name: str
    parent: Optional[str]
    limit: int
    window_seconds: float
    tokens: float
    last_refill: float

    def refill(self, now: float) -> None:
        elapsed = now - self.last_refill
        if elapsed <= 0:
            return
        rate_per_sec = self.limit / self.window_seconds
        self.tokens = min(self.limit, self.tokens + elapsed * rate_per_sec)
        self.last_refill = now


@dataclass
class QuotaStats:
    acquisitions: int = 0
    denials: int = 0
    denials_by_node: Dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, object]:
        return {
            "acquisitions": self.acquisitions,
            "denials": self.denials,
            "denials_by_node": dict(self.denials_by_node),
        }


class QuotaTree:
    """A tree of token buckets where every child consumes from its parent.

    Nodes are referenced by string names. ``parent=None`` designates the
    root. The same name may only appear once in the tree.

    Parameters
    ----------
    clock : callable, optional
        Monotonic-clock callable. Override in tests.
    """

    __slots__ = ("_clock", "_lock", "_buckets", "_children", "_stats")

    def __init__(self, *, clock: callable = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets: Dict[str, _Bucket] = {}
        self._children: Dict[Optional[str], List[str]] = {}
        self._stats = QuotaStats()

    # ------------------------------------------------------------------
    # Tree construction
    # ------------------------------------------------------------------

    def add(self, *, parent: Optional[str], child: str, limit: int, window_seconds: float) -> None:
        """Register a node.

        Parameters
        ----------
        parent : str, optional
            Name of the parent node, or ``None`` for a root.
        child : str
            Name for this node. Must be unique across the tree.
        limit : int
            Maximum tokens per ``window_seconds``. Must be > 0.
        window_seconds : float
            Window length. Must be > 0.
        """
        if limit <= 0:
            raise ValueError("limit must be > 0")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        with self._lock:
            if child in self._buckets:
                raise ValueError(f"node {child!r} already exists")
            if parent is not None and parent not in self._buckets:
                raise ValueError(f"parent {parent!r} not registered")
            now = self._clock()
            self._buckets[child] = _Bucket(
                name=child,
                parent=parent,
                limit=int(limit),
                window_seconds=float(window_seconds),
                tokens=float(limit),  # start full
                last_refill=now,
            )
            self._children.setdefault(parent, []).append(child)

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def remaining(self, name: str, *, now: Optional[float] = None) -> int:
        """Current integer tokens at ``name`` (refilled but not consumed)."""
        ts = self._clock() if now is None else float(now)
        with self._lock:
            b = self._buckets.get(name)
            if b is None:
                raise KeyError(name)
            b.refill(ts)
            return int(b.tokens)

    def __contains__(self, name: str) -> bool:
        with self._lock:
            return name in self._buckets

    def __len__(self) -> int:
        with self._lock:
            return len(self._buckets)

    # ------------------------------------------------------------------
    # Acquisition
    # ------------------------------------------------------------------

    def try_acquire(self, path: List[str], *, cost: int = 1, now: Optional[float] = None) -> QuotaDecision:
        """Atomically reserve ``cost`` tokens at every node along ``path``.

        Walks from root to leaf. If *any* node lacks capacity, the call
        fails and **nothing is consumed** — the partial acquisition is
        rolled back. This is the key invariant: hierarchical acquire is
        all-or-nothing across the path.
        """
        if cost <= 0:
            raise ValueError("cost must be > 0")
        if not path:
            raise ValueError("path must be non-empty")
        ts = self._clock() if now is None else float(now)
        with self._lock:
            # Validate path is a chain.
            for i, name in enumerate(path):
                if name not in self._buckets:
                    raise KeyError(f"unknown node {name!r} at path position {i}")
                if i == 0:
                    expected_parent = None
                else:
                    expected_parent = path[i - 1]
                if self._buckets[name].parent != expected_parent:
                    raise ValueError(
                        f"path break at {name!r}: expected parent "
                        f"{expected_parent!r}, got "
                        f"{self._buckets[name].parent!r}"
                    )
            # Refill all.
            for name in path:
                self._buckets[name].refill(ts)
            # First pass: check capacity. Second pass: consume only if all good.
            shortfalls: Dict[str, int] = {}
            for name in path:
                if self._buckets[name].tokens < cost:
                    shortfalls[name] = cost - int(self._buckets[name].tokens)
            if shortfalls:
                # First failing node is the diagnostic.
                first_blocked = next(iter(shortfalls))
                self._stats.denials += 1
                self._stats.denials_by_node[first_blocked] = (
                    self._stats.denials_by_node.get(first_blocked, 0) + 1
                )
                return QuotaDecision(
                    allowed=False,
                    blocked_at=first_blocked,
                    remaining_by_path={
                        name: int(self._buckets[name].tokens) for name in path
                    },
                    cost=cost,
                )
            # Consume.
            for name in path:
                self._buckets[name].tokens -= cost
            self._stats.acquisitions += 1
            return QuotaDecision(
                allowed=True,
                blocked_at=None,
                remaining_by_path={
                    name: int(self._buckets[name].tokens) for name in path
                },
                cost=cost,
            )

    def peek(self, path: List[str], *, now: Optional[float] = None) -> QuotaDecision:
        """Non-mutating check: would this acquisition succeed?"""
        if not path:
            raise ValueError("path must be non-empty")
        ts = self._clock() if now is None else float(now)
        with self._lock:
            for name in path:
                if name not in self._buckets:
                    raise KeyError(name)
            for name in path:
                self._buckets[name].refill(ts)
            for name in path:
                if self._buckets[name].tokens < 1:
                    return QuotaDecision(
                        allowed=False,
                        blocked_at=name,
                        remaining_by_path={
                            n: int(self._buckets[n].tokens) for n in path
                        },
                        cost=1,
                    )
            return QuotaDecision(
                allowed=True,
                blocked_at=None,
                remaining_by_path={
                    n: int(self._buckets[n].tokens) for n in path
                },
                cost=1,
            )

    def reset(self, path: Optional[List[str]] = None) -> None:
        """Reset one subtree (and all its descendants) to full. ``None`` resets all."""
        with self._lock:
            if path is None:
                now = self._clock()
                for b in self._buckets.values():
                    b.tokens = float(b.limit)
                    b.last_refill = now
                return
            target_root = path[-1]
            to_reset = self._collect_subtree(target_root)
            now = self._clock()
            for name in to_reset:
                b = self._buckets.get(name)
                if b is not None:
                    b.tokens = float(b.limit)
                    b.last_refill = now

    def _collect_subtree(self, root_name: str) -> List[str]:
        out = [root_name]
        stack = [root_name]
        while stack:
            n = stack.pop()
            for c in self._children.get(n, []):
                out.append(c)
                stack.append(c)
        return out

    def stats(self) -> QuotaStats:
        with self._lock:
            return QuotaStats(
                acquisitions=self._stats.acquisitions,
                denials=self._stats.denials,
                denials_by_node=dict(self._stats.denials_by_node),
            )

    def reset_stats(self) -> None:
        with self._lock:
            self._stats = QuotaStats()


def acquire_or_raise(tree: QuotaTree, path: List[str], *, cost: int = 1) -> None:
    """Helper: try_acquire and raise :class:`QuotaExceeded` on denial."""
    d = tree.try_acquire(path, cost=cost)
    if not d.allowed:
        raise QuotaExceeded(d.blocked_at or "?", d)


__all__ = [
    "__version__",
    "QuotaTree",
    "QuotaDecision",
    "QuotaExceeded",
    "QuotaStats",
    "acquire_or_raise",
]
