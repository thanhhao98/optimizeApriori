from utils import SupportRecord, RelationRecord, OrderedStatistic, TransactionManager
from itertools import combinations
# from utils import generateData, drawPlot



def create_next_candidates(prev_candidates, length):
    item_set = set()
    for candidate in prev_candidates:
        for item in candidate:
            item_set.add(item)
    items = sorted(item_set)
    tmp_next_candidates = (frozenset(x) for x in combinations(items, length))
    if length < 3:
        return list(tmp_next_candidates)
    next_candidates = [
        candidate for candidate in tmp_next_candidates
        if all(
            True if frozenset(x) in prev_candidates else False
            for x in combinations(candidate, length - 1))
    ]
    return next_candidates


def gen_support_records(transaction_manager, min_support, **kwargs):
    max_length = kwargs.get('max_length')

    _create_next_candidates = kwargs.get(
        '_create_next_candidates', create_next_candidates)

    candidates = transaction_manager.initial_candidates()
    length = 1
    while candidates:
        relations = set()
        for relation_candidate in candidates:
            support = transaction_manager.calc_support(relation_candidate)
            if support < min_support:
                continue
            candidate_set = frozenset(relation_candidate)
            relations.add(candidate_set)
            yield SupportRecord(candidate_set, support)
        length += 1
        if max_length and length > max_length:
            break
        candidates = _create_next_candidates(relations, length)


def gen_ordered_statistics(transaction_manager, record):
    items = record.items
    for combination_set in combinations(sorted(items), len(items) - 1):
        items_base = frozenset(combination_set)
        items_add = frozenset(items.difference(items_base))
        confidence = (
            record.support / transaction_manager.calc_support(items_base))
        lift = confidence / transaction_manager.calc_support(items_add)
        yield OrderedStatistic(
            frozenset(items_base), frozenset(items_add), confidence, lift)


def filter_ordered_statistics(ordered_statistics, **kwargs):
    min_confidence = kwargs.get('min_confidence', 0.0)
    min_lift = kwargs.get('min_lift', 0.0)

    for ordered_statistic in ordered_statistics:
        if ordered_statistic.confidence < min_confidence:
            continue
        if ordered_statistic.lift < min_lift:
            continue
        yield ordered_statistic


def apriori(transactions, items, **kwargs):
    # Parse the arguments.
    numReduce = kwargs.get('numReduce', 15)
    min_support = kwargs.get('min_support', 0.1)
    min_confidence = kwargs.get('min_confidence', 0.0)
    min_lift = kwargs.get('min_lift', 0.0)
    max_length = kwargs.get('max_length', None)

    # Check arguments.
    if min_support <= 0:
        raise ValueError('minimum support must be > 0')

    # For testing.
    _gen_support_records = kwargs.get(
        '_gen_support_records', gen_support_records)
    _gen_ordered_statistics = kwargs.get(
        '_gen_ordered_statistics', gen_ordered_statistics)
    _filter_ordered_statistics = kwargs.get(
        '_filter_ordered_statistics', filter_ordered_statistics)

    # Calculate supports.
    transaction_manager = TransactionManager(transactions, items, numReduce)
    support_records = _gen_support_records(
        transaction_manager, min_support, max_length=max_length)
    # Calculate ordered stats.
    for support_record in support_records:
        ordered_statistics = list(
            _filter_ordered_statistics(
                _gen_ordered_statistics(transaction_manager, support_record),
                min_confidence=min_confidence,
                min_lift=min_lift,
            )
        )
        if not ordered_statistics:
            continue
        yield RelationRecord(
            support_record.items, support_record.support, ordered_statistics)