import random
from collections import Counter, deque

import pytest

from src.part4.bloom_filter import BloomFilter, optimal_hashes, theoretical_fpr
from src.part4.count_min_sketch import CountMinSketch, delta, epsilon
from src.part4.dgim import DGIM


def test_dgim_time_window_respects_bound():
    rng = random.Random(3)
    for window in [1, 7, 30]:
        dgim, history, day = DGIM(window), deque(), 0
        for _ in range(5000):
            day += rng.random() < 0.3  # varios eventos por día
            bit = rng.random() < 0.4
            dgim.add(bit, day)
            history.append((day, bit))
            while history and history[0][0] <= day - window:
                history.popleft()
            exact = sum(b for _, b in history)
            assert abs(dgim.query() - exact) <= max(0.5, 0.5 * exact)


def test_bloom_has_no_false_negatives_and_fpr_near_theory():
    bloom = BloomFilter(2**14, 5)
    inserted = [f'in{i}' for i in range(1500)]
    for value in inserted:
        bloom.add(value)
    assert all(value in bloom for value in inserted)
    observed = sum(f'out{i}' in bloom for i in range(20000)) / 20000
    assert observed == pytest.approx(theoretical_fpr(2**14, 5, 1500), abs=0.01)
    assert optimal_hashes(2**14, 1500) == 8


def test_count_min_never_underestimates_and_meets_bound():
    rng = random.Random(5)
    values = [f'k{int(rng.paretovariate(1.2))}' for _ in range(20000)]
    sketch, counts = CountMinSketch(256, 4), Counter(values)
    for value in values:
        sketch.add(value)
    errors = [sketch.query(k) - c for k, c in counts.items()]
    assert min(errors) >= 0
    within = sum(e <= epsilon(256) * len(values) for e in errors) / len(errors)
    assert within >= 1 - delta(4)
    assert sketch.total == len(values)
