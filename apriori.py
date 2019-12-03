from utils import SupportRecord, RelationRecord, OrderedStatistic, TransactionManager
from itertools import combinations
from apyori import apriori as apyori
import time
import json


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


if __name__ == '__main__':
    with open('transactions.json','rb') as f:
        rTransactions = json.load(f)
    with open('items.json','rb') as f:
        items = json.load(f)
    
    # gTransactions = generateData(items,500000)
    transactions = rTransactions

    min_support = 0.02
    min_confidence = 0.1
    min_lift = 0.0
    max_length = None

    start = time.time()
    results2 = list(apriori(transactions=transactions, items=items, min_confidence=min_confidence,min_support=min_support,min_lift=min_lift,numReduce=1))
    t2 = time.time()-start
    print(len(results2))
    print(t2)
    print()
    for i in results2[:5]:
        print(i)


    start = time.time()
    results1 = list(apyori(transactions,min_confidence=min_confidence,min_support=min_support,min_lift=min_lift))
    t1 = time.time()-start
    print(len(results1))
    print(t1)
    for i in results1[:5]:
        print(i)

    print(t1/t2)

    # items = list(map(lambda x: str(x), range(10000)))
    # numDatas = [1000]#,10000,20000,30000]#,40000,50000,60000,70000,80000,90000,100000]
    # r1 = []
    # r2 = []
    # s = []
    # for numData in numDatas:
    #     gTransactions = generateData(items,numData)
    #     transactions = gTransactions

    #     min_support = 0.02
    #     min_confidence = 0.1
    #     min_lift = 0.0
    #     max_length = None

    #     start = time.time()
    #     results1 = list(apyori(transactions,min_confidence=min_confidence,min_support=min_support,min_lift=min_lift))
    #     t1 = time.time()-start
    #     r1.append(t1)

        
    #     start = time.time()
    #     results2 = list(apriori(transactions=transactions, items=items, min_confidence=min_confidence,min_support=min_support,min_lift=min_lift))
    #     t2 = time.time()-start
    #     r2.append(t2)
    #     s.append(t1/t2)
    # print(r1)

    # print(r2)

    # print(s)