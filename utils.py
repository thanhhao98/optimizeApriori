from itertools import combinations
from functools import reduce
from collections import namedtuple
from bloomFilter import BloomFilter
import matplotlib.pyplot as plt
import random
import json



SupportRecord = namedtuple('SupportRecord', ('items', 'support'))
RelationRecord = namedtuple('RelationRecord', SupportRecord._fields + ('ordered_statistics',))
OrderedStatistic = namedtuple('OrderedStatistic', ('items_base', 'items_add', 'confidence', 'lift',))


def loadBaseDataSet():
    with open('transactions.json','rb') as f:
        transactions = json.load(f)
    with open('items.json','rb') as f:
        items = json.load(f)
    return transactions, items


def backup(numDatas,target,optimizedLib):
    status = len(numDatas)==len(target)
    resultCompareSpeed = {
        'status': status,
        'numDatas': numDatas,
        'target': target,
        'optimizedLib': optimizedLib
    }
    with open('resultCompareSpeed.json', 'w') as outfile:
        json.dump(resultCompareSpeed, outfile)


def drawPlot(x, listy, labels, labelD):
    for i in zip(listy, labels):
        plt.plot(x, i[0], label=i[1])
    plt.xlabel(labelD['x'])
    plt.ylabel(labelD['y'])
    plt.legend()
    return plt


def generateData(items, numData, minItems=2, maxItems=20):
    gTransactions = []
    for i in range(numData):
        numItems = random.randint(minItems, maxItems)
        t = random.sample(items, numItems)
        gTransactions.append(t)
    return gTransactions


class TransactionManager(object):
    def __init__(self, transactions, items, numReduce):
        self.__numReduce = numReduce
        self.__num_transaction = len(transactions)
        self.__items = items
        self.__transaction_index_map = {}
        self.__listHashFunc = BloomFilter(int(self.__num_transaction/self.__numReduce), 0.05).listHashFunc
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


def getNumIntersectionWith(bls):
    numBl = len(bls)
    ids = list(range(numBl))
    combBl = []
    for i in ids:
        combBl += combinations(bls, i+1)
    results = list(map(lambda bls: getUnionWithMany(bls).getNummElement(), combBl))
    signs = list(map(lambda bls: 1 if len(bls) % 2 else -1, combBl))
    data = list(zip(signs, results))
    return reduce(lambda x, y: y[0]*y[1] + x, data, 0)


def getUnionWithMany(listBl):
    listBl = list(listBl)
    bls = listBl[1:]
    root = listBl[0]
    return root.getUnionWithMany(bls)


def workerHashFunc(results, idFrom, idTo, listHashFunc):
    for i in range(idFrom, idTo):
        for j in range(len(listHashFunc)):
            results[i][j] = listHashFunc[j](str(i))


def worker(transactions, transaction_index_map):
    for i, transaction in enumerate(transactions):
        for item in transaction:
            transaction_index_map[item].add(str(i))