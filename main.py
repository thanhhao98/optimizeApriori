from apriori import apriori as targetApriori
from apyori import apriori as pyLibOptimizedApriori
from utils import generateData, drawPlot, backup, loadBaseDataSet
import time


def testSpeed():
    _,items = loadBaseDataSet()
    numDatas = [1000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000, 200000, 400000, 600000, 800000, 1000000]
    target = []
    optimizedLib = []
    for numData in numDatas:
        gTransactions = generateData(items, numData)
        transactions = gTransactions
        print(f'Running with {len(transactions)}-dataset'.center(100,' '))
        min_support = 0.02
        min_confidence = 0.1
        min_lift = 0.0

        start1 = time.time()
        result1 = list(targetApriori(transactions=transactions, items=items, min_confidence=min_confidence,min_support=min_support,min_lift=min_lift,numReduce=5))
        end1 = time.time()
        target.append(end1-start1)

        start2 = time.time()
        result2 = list(pyLibOptimizedApriori(transactions,min_confidence=min_confidence,min_support=min_support,min_lift=min_lift))
        end2 = time.time()
        optimizedLib.append(end2-start2)
        print('Backing up...')
        backup(numDatas, target, optimizedLib)
    labels = ['target', 'optimizedLib']
    labelD = {'y': 'Time (s)', 'x': 'Num Transactions'}
    backup(numDatas, target, optimizedLib)
    drawPlot(numDatas, [target, optimizedLib], labels, labelD).savefig('reslut.png')
    print('Done!'.center(100,' '))


def checkIsValid():
    transactions, items = loadBaseDataSet()

    min_support = 0.02
    min_confidence = 0.1
    min_lift = 0.0

    results1 = list(targetApriori(transactions=transactions, items=items, min_confidence=min_confidence,min_support=min_support,min_lift=min_lift,numReduce=1))
    print(results1[:2])
    results2 = list(pyLibOptimizedApriori(transactions,min_confidence=min_confidence,min_support=min_support,min_lift=min_lift))
    print(results1[:1])


if __name__ == '__main__':
    # checkIsValid()
    testSpeed()
