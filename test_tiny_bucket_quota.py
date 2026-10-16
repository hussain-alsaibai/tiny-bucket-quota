"""Tests for tiny_bucket_quota v0.1.0."""

from __future__ import annotations

import unittest

from tiny_bucket_quota import (
    QuotaDecision,
    QuotaExceeded,
    QuotaTree,
    __version__,
    acquire_or_raise,
)


class _Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class TestVersion(unittest.TestCase):
    def test_version(self) -> None:
        self.assertEqual(__version__, "0.1.0")


class TestConstruction(unittest.TestCase):
    def test_construct_root(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="root", limit=100, window_seconds=60)
        self.assertIn("root", tree)
        self.assertEqual(len(tree), 1)

    def test_construct_chain(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="org", limit=500, window_seconds=60)
        tree.add(parent="org", child="team", limit=200, window_seconds=60)
        tree.add(parent="team", child="user", limit=50, window_seconds=60)
        self.assertEqual(len(tree), 3)

    def test_rejects_zero_limit(self) -> None:
        tree = QuotaTree()
        with self.assertRaises(ValueError):
            tree.add(parent=None, child="x", limit=0, window_seconds=60)

    def test_rejects_zero_window(self) -> None:
        tree = QuotaTree()
        with self.assertRaises(ValueError):
            tree.add(parent=None, child="x", limit=10, window_seconds=0)

    def test_rejects_unknown_parent(self) -> None:
        tree = QuotaTree()
        with self.assertRaises(ValueError):
            tree.add(parent="ghost", child="x", limit=10, window_seconds=60)

    def test_rejects_duplicate_child(self) -> None:
        tree = QuotaTree()
        tree.add(parent=None, child="x", limit=10, window_seconds=60)
        with self.assertRaises(ValueError):
            tree.add(parent=None, child="x", limit=10, window_seconds=60)


class TestAcquire(unittest.TestCase):
    def test_basic_allow(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="root", limit=10, window_seconds=60)
        d = tree.try_acquire(["root"])
        self.assertTrue(d.allowed)
        self.assertEqual(d.remaining_by_path, {"root": 9})

    def test_drains_leaf(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="root", limit=10, window_seconds=60)
        tree.add(parent="root", child="leaf", limit=3, window_seconds=60)
        for i in range(3):
            d = tree.try_acquire(["root", "leaf"])
            self.assertTrue(d.allowed, f"iteration {i+1}")
        d_blocked = tree.try_acquire(["root", "leaf"])
        self.assertFalse(d_blocked.allowed)
        self.assertEqual(d_blocked.blocked_at, "leaf")

    def test_blocks_at_parent(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="root", limit=2, window_seconds=60)
        tree.add(parent="root", child="leaf", limit=10, window_seconds=60)
        # Drain root to 1.
        tree.try_acquire(["root"])
        d = tree.try_acquire(["root", "leaf"], cost=2)
        self.assertFalse(d.allowed)
        self.assertEqual(d.blocked_at, "root")
        # Root still has 1 — confirm no consumption happened.
        self.assertEqual(tree.remaining("root"), 1)
        self.assertEqual(tree.remaining("leaf"), 10)

    def test_all_or_nothing(self) -> None:
        """A failed hierarchical acquire consumes nothing at any level."""
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="root", limit=2, window_seconds=60)
        tree.add(parent="root", child="leaf", limit=10, window_seconds=60)
        tree.try_acquire(["root"])
        tree.try_acquire(["root", "leaf"])  # root now has 0
        d = tree.try_acquire(["root", "leaf"])
        self.assertFalse(d.allowed)
        # Leaf still untouched at 9.
        self.assertEqual(tree.remaining("leaf"), 9)

    def test_refill(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="root", limit=60, window_seconds=60)
        # Drain.
        for _ in range(60):
            tree.try_acquire(["root"])
        d = tree.try_acquire(["root"])
        self.assertFalse(d.allowed)
        # Advance 30s — should refill 30 tokens.
        clk.advance(30)
        d = tree.try_acquire(["root"])
        self.assertTrue(d.allowed)
        self.assertGreater(d.remaining_by_path["root"], 0)

    def test_cost_exceeds_leaf_blocks(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="root", limit=100, window_seconds=60)
        tree.add(parent="root", child="leaf", limit=5, window_seconds=60)
        d = tree.try_acquire(["root", "leaf"], cost=10)
        self.assertFalse(d.allowed)
        self.assertEqual(d.blocked_at, "leaf")


class TestPeek(unittest.TestCase):
    def test_peek_does_not_consume(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="root", limit=10, window_seconds=60)
        d1 = tree.peek(["root"])
        d2 = tree.peek(["root"])
        self.assertTrue(d1.allowed)
        self.assertEqual(d1.remaining_by_path, d2.remaining_by_path)


class TestReset(unittest.TestCase):
    def test_reset_subtree(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="org", limit=10, window_seconds=60)
        tree.add(parent="org", child="team", limit=5, window_seconds=60)
        tree.add(parent="team", child="user", limit=2, window_seconds=60)
        # Drain user.
        tree.try_acquire(["org", "team", "user"])
        tree.try_acquire(["org", "team", "user"])
        # Reset user only.
        tree.reset(["user"])
        self.assertEqual(tree.remaining("user"), 2)
        # Org and team should still be drained.
        self.assertEqual(tree.remaining("org"), 8)
        self.assertEqual(tree.remaining("team"), 3)

    def test_reset_subtree_with_descendants(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="org", limit=10, window_seconds=60)
        tree.add(parent="org", child="team", limit=5, window_seconds=60)
        tree.add(parent="team", child="user", limit=2, window_seconds=60)
        tree.reset(["org"])
        self.assertEqual(tree.remaining("org"), 10)
        self.assertEqual(tree.remaining("team"), 5)
        self.assertEqual(tree.remaining("user"), 2)

    def test_reset_all(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="a", limit=5, window_seconds=60)
        tree.add(parent="a", child="b", limit=2, window_seconds=60)
        tree.try_acquire(["a", "b"])
        tree.reset()
        self.assertEqual(tree.remaining("a"), 5)
        self.assertEqual(tree.remaining("b"), 2)


class TestStats(unittest.TestCase):
    def test_stats_counts(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="root", limit=2, window_seconds=60)
        tree.add(parent="root", child="leaf", limit=1, window_seconds=60)
        tree.try_acquire(["root", "leaf"])  # ok
        tree.try_acquire(["root", "leaf"])  # blocked
        s = tree.stats()
        self.assertEqual(s.acquisitions, 1)
        self.assertEqual(s.denials, 1)
        self.assertEqual(s.denials_by_node.get("leaf"), 1)


class TestHelper(unittest.TestCase):
    def test_acquire_or_raise(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="root", limit=1, window_seconds=60)
        acquire_or_raise(tree, ["root"])
        with self.assertRaises(QuotaExceeded) as cm:
            acquire_or_raise(tree, ["root"])
        self.assertEqual(cm.exception.blocked_at, "root")


class TestPathValidation(unittest.TestCase):
    def test_rejects_path_break(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="a", limit=10, window_seconds=60)
        tree.add(parent=None, child="b", limit=10, window_seconds=60)
        tree.add(parent="a", child="c", limit=10, window_seconds=60)
        # Try to walk b->c; c's parent is a, not b.
        with self.assertRaises(ValueError):
            tree.try_acquire(["b", "c"])

    def test_rejects_empty_path(self) -> None:
        tree = QuotaTree()
        with self.assertRaises(ValueError):
            tree.try_acquire([])

    def test_rejects_zero_cost(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="x", limit=10, window_seconds=60)
        with self.assertRaises(ValueError):
            tree.try_acquire(["x"], cost=0)

    def test_unknown_node_raises(self) -> None:
        clk = _Clock()
        tree = QuotaTree(clock=clk)
        tree.add(parent=None, child="x", limit=10, window_seconds=60)
        with self.assertRaises(KeyError):
            tree.try_acquire(["x", "ghost"])


if __name__ == "__main__":
    unittest.main()
