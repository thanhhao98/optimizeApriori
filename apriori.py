from bloomFilter import BloomFilter, getNumIntersectionWith
from collections import namedtuple
from itertools import combinations
from apyori import apriori as apyori
from threading import Thread
import numpy as np
import time
import json
import random
from functools import reduce


# def workerHashFunc(results, idFrom, idTo, listHashFunc):
#     for i in range(idFrom, idTo):
#         for j in range(len(listHashFunc)):
#             results[i][j] = listHashFunc[j](str(i))

# def worker(transactions, transaction_index_map):
#     for i, transaction in enumerate(transactions):
#         for item in transaction:
#             transaction_index_map[item].add(str(i))


class TransactionManager(object):
    """
    Transaction managers.
    """

    def __init__(self, transactions, items):
        self.__numReduce = 15
        self.__num_transaction = len(transactions)
        self.__items = items
        self.__transaction_index_map = {}
        self.__listHashFunc = BloomFilter(int(self.__num_transaction/self.__numReduce), 0.05).listHashFunc
        # start = time.time()
        # numThread = 3
        # threads = []
        # numOnThread = int(self.__num_transaction / numThread) + 1
        # listResult = np.zeros((self.__num_transaction, len(self.__listHashFunc)), dtype=np.int)
        # for i in range(numThread):
        #     idFrom = i*numOnThread
        #     if i == numThread-1:
        #         idTo = self.__num_transaction
        #     else:
        #         idTo = (i+1)*numOnThread
        #     t = Thread(
        #         target=workerHashFunc,
        #         args=[listResult, idFrom, idTo, self.__listHashFunc],
        #     )
        #     threads.append(t)
        #     t.start()
        # for t in threads:
        #     t.join()
        # transactionsHashed = listResult.tolist()
        # print(time.time()-start)
        transactionsHashed = list()
        for i in range(self.__num_transaction):
            transactionsHashed.append(list(map(lambda y: y(str(i)), self.__listHashFunc )))
        for hashed, transaction in zip(transactionsHashed,transactions):
            self.add_transaction(transaction,hashed)

    def add_transaction(self, transaction, hashed):
        for item in transaction:
            if item not in self.__transaction_index_map:
                self.__transaction_index_map[item] = BloomFilter(int(self.__num_transaction/self.__numReduce), 0.05)
            self.__transaction_index_map[item].addHashed(hashed)

    def calc_support(self, items):
        if not items:
            return 1.0
        if not self.num_transaction:
            return 0.0
        bls = [self.__transaction_index_map.get(item) for item in items]
        return getNumIntersectionWith(bls)/self.__num_transaction

    def initial_candidates(self):
        return [frozenset([item]) for item in self.items]

    @property
    def num_transaction(self):
        return self.__num_transaction

    @property
    def items(self):
        return sorted(self.__items)


SupportRecord = namedtuple('SupportRecord', ('items', 'support'))
RelationRecord = namedtuple('RelationRecord', SupportRecord._fields + ('ordered_statistics',))
OrderedStatistic = namedtuple('OrderedStatistic', ('items_base', 'items_add', 'confidence', 'lift',))


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
    transaction_manager = TransactionManager(transactions, items)
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


def generateData(items, numData, minItems=2, maxItems=20):
    gTransactions = []
    for i in range(numData):
        numItems = random.randint(minItems, maxItems)
        t = random.sample(items, numItems)
        gTransactions.append(t)
    return gTransactions

if __name__ == '__main__':
    with open('transactions.json','rb') as f:
        rTransactions = json.load(f)
    with open('items.json','rb') as f:
        items = json.load(f)
    
    gTransactions = generateData(items,5000000)
    transactions = gTransactions

    min_support = 0.01
    min_confidence = 0.1
    min_lift = 0.0
    max_length = None

    start = time.time()
    results2 = list(apriori(transactions=transactions, items=items, min_confidence=min_confidence,min_support=min_support,min_lift=min_lift))
    t2 = time.time()-start
    print(len(results2))
    print(t2)
    print()


    start = time.time()
    results1 = list(apyori(transactions,min_confidence=min_confidence,min_support=min_support,min_lift=min_lift))
    t1 = time.time()-start
    print(len(results1))
    print(t1)

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