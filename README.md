# tiny-bucket-quota

**Hierarchical token-bucket quota allocator.** Org → team → user budgets that auto-balance.

Stdlib only. MIT licensed. ~370 LOC. 23 tests.

```bash
pip install tiny-bucket-quota
```

---

## Why hierarchical?

A flat per-key token bucket works for one dimension (one API key gets
1000 RPM). Real workloads are **nested**:

```
org:acme (5000 RPM)
├── team:backend (2000 RPM)
│   ├── user:42 (200 RPM)
│   └── user:43 (200 RPM)
└── team:frontend (3000 RPM)
    ├── user:alice (500 RPM)
    └── user:bob   (500 RPM)
```

When user:42 spends 200 RPM, team:backend must drop to 1800 and org:acme
to 4800. A flat limiter on user:42 alone would let the team burn through
4000 RPM total (200 × 20) — twice the team budget.

`tiny-bucket-quota` implements exactly that: a tree of token buckets
where every child consumes from its parent, and **hierarchical acquire
is all-or-nothing** — if any level has zero capacity, nothing is
consumed.

---

## Quick start

```python
from tiny_bucket_quota import QuotaTree, acquire_or_raise, QuotaExceeded

tree = QuotaTree()
tree.add(parent=None,         child="org:acme",     limit=5000, window_seconds=60)
tree.add(parent="org:acme",   child="team:backend", limit=2000, window_seconds=60)
tree.add(parent="team:backend", child="user:42",    limit=50,   window_seconds=60)

try:
    acquire_or_raise(tree, ["org:acme", "team:backend", "user:42"])
    call_external_api()
except QuotaExceeded as e:
    return {"error": "rate_limited", "blocked_at": e.blocked_at}, 429
```

---

## Public API

### `QuotaTree(*, clock=time.monotonic)`

The tree. One lock; thread-safe.

### `tree.add(parent, child, *, limit, window_seconds)`

Register a node. `parent=None` makes the node a root. Each name appears
once. Cycles are not possible (parent must already be registered).

### `tree.try_acquire(path, *, cost=1, now=None) -> QuotaDecision`

Atomically reserve `cost` tokens at every node along `path`. The
acquisition is **all-or-nothing**: if any level lacks capacity, nothing
is consumed. The returned `QuotaDecision` includes:

- `allowed: bool`
- `blocked_at: Optional[str]` — first failing node
- `remaining_by_path: Dict[str, int]` — remaining at each walked node
- `cost: int`

### `tree.peek(path, *, now=None) -> QuotaDecision`

Non-mutating check.

### `tree.reset(path=None)`

Reset one subtree (and all its descendants) to full. `None` resets all.

### `tree.stats() -> QuotaStats`

Aggregate counters: `acquisitions`, `denials`, `denials_by_node`.

---

## Why this, not three flat `tiny-rate-limiter`s?

You *can* stack three `tiny-rate-limiter`s at org/team/user. The problem
is they don't share state — user:42 could spend 50 RPM while the team
bucket sits full at 2000, and you'd never notice the team is heading
for burnout. `tiny-bucket-quota` shares state across levels by design.

If your hierarchy is shallow (one level) and you only care about the
leaf, `tiny-rate-window` or `tiny-rate-limiter` are simpler.

---

## Composition

```python
# Refuse an API call if the org is already past 90% of its daily budget.
tree = QuotaTree()
tree.add(parent=None, child="org", limit=10_000, window_seconds=86400)

if tree.remaining("org") < 1000:
    raise OrgBudgetExhausted()
```

Pair with `tiny-cost-tracker` (USD-denominated spend) for hybrid
budgets where some dimensions are RPM and others are dollars.

---

## Performance

A single `try_acquire` walks the path (typically 2–5 nodes), refills
each bucket, checks capacity, and (on success) decrements each. Under a
single short-held lock. On a 2024 MacBook Pro, ~120K acquires/sec on
3-level paths with 10K nodes.

For very wide fan-outs (>1000 children per node), consider flattening
the hierarchy or sharding the lock by top-level node.

---

## License

MIT — copy, fork, ship.
