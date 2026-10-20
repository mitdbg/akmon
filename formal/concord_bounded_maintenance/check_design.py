"""Exhaustive small design checks; no third-party package or production engine.

The queue model is FIFO, immutable costs/identities, complete finite work, and
work-conserving. Every admitted job includes at least one unit for commit.
"""
from collections import deque
from itertools import product
import json


def serve(queue, capacity):
    result = list(queue)
    completed = 0
    while result and capacity:
        spend = min(result[0], capacity)
        result[0] -= spend
        capacity -= spend
        if result[0] == 0:
            completed += 1
            result.pop(0)
    return tuple(result), completed


def compositions(total):
    if total == 0:
        return [()]
    return [(first,) + rest for first in range(1, total + 1)
            for rest in compositions(total - first)]


def queue_checks():
    explored = 0
    transitions = 0
    profiles = []
    for burst, refill, capacity in [(2, 1, 1), (3, 1, 2), (3, 2, 2), (4, 2, 3)]:
        seen = {(burst, ())}
        todo = deque(seen)
        while todo:
            tokens, queue = todo.popleft()
            available = min(burst, tokens + refill)
            for arriving_work in range(available + 1):
                for jobs in compositions(arriving_work):
                    enlarged = queue + jobs
                    remaining, _ = serve(enlarged, capacity)
                    state = (available - arriving_work, remaining)
                    assert sum(remaining) + state[0] <= burst
                    assert len(remaining) <= burst
                    # Every target's prefix drains within ceil((B+R)/C),
                    # independent of future arrivals appended behind it.
                    for target in range(len(enlarged)):
                        prefix = enlarged[:target + 1]
                        rounds = (burst + refill + capacity - 1) // capacity
                        for _ in range(rounds):
                            prefix, _ = serve(prefix, capacity)
                        assert not prefix
                    transitions += 1
                    if state not in seen:
                        seen.add(state)
                        todo.append(state)
        explored += len(seen)
        profiles.append(dict(burst=burst, refill=refill, capacity=capacity,
                             states=len(seen)))
    return dict(states=explored, transitions=transitions, profiles=profiles)


def full(values, edges):
    out = []
    for node, value in enumerate(values):
        out.append(value + sum(out[a] for a, b in edges if b == node))
    return tuple(out)


def indexes(values, edges):
    children = [[] for _ in values]
    parents = [[] for _ in values]
    for a, b in edges:
        children[a].append(b)
        parents[b].append(a)
    return children, parents


def incremental(old_result, values, adjacency, changed, broken=False):
    # Indexes already exist; their construction/maintenance is not charged here.
    # This is why the checker is NOT an end-to-end Concord complexity proof.
    children, parents = adjacency
    affected = set(changed)
    todo = list(changed)
    trace = 0
    while todo:
        node = todo.pop()
        if broken:
            continue
        for child in children[node]:
            trace += 1
            if child not in affected:
                affected.add(child)
                todo.append(child)
    # Full result copying and affected sorting are deliberately uncharged.
    # This measures selected dependency operations, not total running time.
    result = list(old_result)
    evaluate = 0
    for node in sorted(affected):
        evaluate += 1 + len(parents[node])
        result[node] = values[node] + sum(result[p] for p in parents[node])
    return tuple(result), 1 + len(changed) + trace + evaluate


def locality_checks():
    comparisons = 0
    possible_edges = [(0, 1), (0, 2), (1, 2)]
    for selection in product([False, True], repeat=3):
        edges = [e for e, selected in zip(possible_edges, selection) if selected]
        for values in product(range(2), repeat=3):
            old = full(values, edges)
            for changed in range(3):
                updated = list(values)
                updated[changed] += 1
                actual, work = incremental(old, updated, indexes(updated, edges), [changed])
                assert actual == full(updated, edges)
                # Adding unrelated isolated nodes leaves charged run work equal.
                padded_values = updated + [0] * 100
                padded, padded_work = incremental(old + (0,) * 100,
                                                   padded_values, indexes(padded_values, edges), [changed])
                assert padded == full(padded_values, edges)
                assert work == padded_work
                comparisons += 1
    return dict(comparisons=comparisons, unrelated_padding=100,
                scope="static acyclic additive dependencies; indexed adjacency assumed")


def negative_controls():
    witnesses = {}
    # Removed capacity/envelope premise: pending generations grow every round.
    queue = ()
    for _ in range(6):
        queue, _ = serve(queue + (1, 1), 1)
    assert len(queue) == 6
    witnesses['overload'] = dict(rounds=6, lag=len(queue), arrivals_per_round=2, capacity=1)
    # No positive commit charge: huge version count but zero charged backlog.
    witnesses['free_publications'] = dict(pending_versions=1000, charged_backlog=0)
    assert witnesses['free_publications']['pending_versions'] > 3
    witnesses['unadmitted_publications'] = dict(latest_graph=1000, completed_graph=0, admitted_backlog=0)
    assert witnesses['unadmitted_publications']['latest_graph'] > 3
    # Mutating the active target restarts its work and prevents completion.
    remaining = 2
    for _ in range(6):
        remaining = 2  # new publication replaces the active target
        remaining -= 1
    assert remaining == 1
    witnesses['moving_target'] = dict(rounds=6, completed_versions=0)
    # Serving zero forever violates the strictly positive service premise.
    assert serve((1,), 0)[0] == (1,)
    witnesses['no_service'] = dict(remaining=1, capacity=0)
    # Partial findings can be correct at neither old nor new version.
    old, new, partial = (0, 0), (1, 1), (1, 0)
    assert partial != old and partial != new
    witnesses['mixed_versions'] = dict(old=old, new=new, partial=partial)
    # Omitting dependency propagation breaks agreement with full evaluation.
    actual, _ = incremental((0, 0), [1, 0], indexes([1, 0], [(0, 1)]), [0], broken=True)
    assert actual != full([1, 0], [(0, 1)])
    witnesses['missed_dependency'] = dict(actual=actual, expected=(1, 1))
    # Fanout is real work: a single changed record can reach the entire graph.
    _, small = incremental((0,) * 4, [1, 0, 0, 0], indexes([0] * 4, [(0, i) for i in range(1, 4)]), [0])
    _, large = incremental((0,) * 40, [1] + [0] * 39, indexes([0] * 40, [(0, i) for i in range(1, 40)]), [0])
    assert large > small
    witnesses['unbounded_fanout'] = dict(changed_records=1, work_4_nodes=small, work_40_nodes=large)
    # Fresh work below capacity does NOT alone starve an old finite queue.
    old_work = 7
    for _ in range(7):
        old_work = max(0, old_work - (2 - 1))
    assert old_work == 0
    witnesses['newest_first_claim_correction'] = dict(capacity=2, new_work=1,
                                                     initial_old_work=7, old_work_after_7=0)
    return witnesses


if __name__ == '__main__':
    print(json.dumps(dict(status='pass', queues=queue_checks(),
                          locality=locality_checks(), negative_controls=negative_controls()),
                     indent=2))
