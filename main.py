from apriori import apriori as targetApriori
from apyori import apriori as pyLibOptimizedApriori
from utils import generateData, drawPlot, backup, loadBaseDataSet
from pureApriori import apriori as pureApriori
import time


def testSpeedWithLibMinSub():
    minSupports = [0.5, 0.2, 0.1, 0.05, 0.02, 0.01, 0.008, 0.005, 0.002, 0.001, 0.0008, 0.0005]
    lable = [1/x for x  in minSupports]
    fileBackup = 'compareMinSup/backup.json'
    transactions,items = loadBaseDataSet()
    target = []
    optimizedLib = []
    for min_support in minSupports:
        print(f'Running with {min_support}-min_support'.center(100,' '))
        min_confidence = 0.1
        min_lift = 0.0

        start1 = time.time()
        result1 = list(targetApriori(transactions=transactions, items=items, min_confidence=min_confidence,min_support=min_support,min_lift=min_lift,numReduce=0.1))
        end1 = time.time()
        target.append(end1-start1)

        start2 = time.time()
        result2 = list(pyLibOptimizedApriori(transactions,min_confidence=min_confidence,min_support=min_support,min_lift=min_lift))
        end2 = time.time()
        optimizedLib.append(end2-start2)
        print(len(result1))
        print(len(result2))
        print('Backing up...')
        backup(fileBackup, minSupports, target, optimizedLib)
    labels = ['target','optimizedLib']
    labelD = {'y': 'Time (s)', 'x': '1/min_support'}
    backup(fileBackup,minSupports, target, optimizedLib)
    drawPlot(lable, [optimizedLib,target], labels, labelD).savefig('compareMinSup/reslut.png')
    print('Done!'.center(100,' '))



def testSpeedWithLib():
    numDatas = [1000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000, 200000, 400000, 600000, 800000, 1000000]
    fileBackup = 'compareLibApyori/backup.json'
    _,items = loadBaseDataSet()
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
        backup(fileBackup, numDatas, target, optimizedLib)
    labels = ['target', 'optimizedLib']
    labelD = {'y': 'Time (s)', 'x': 'Num Transactions'}
    backup(fileBackup,numDatas, target, optimizedLib)
    drawPlot(numDatas, [target, optimizedLib], labels, labelD).savefig('compareLibApyori/reslut.png')
    print('Done!'.center(100,' '))

def testSpeedWithNormal():
    fileBackup = 'compareNormal/backup.json'
    _,items = loadBaseDataSet()
    numDatas = [1000, 1500, 2000, 2500, 3000, 4000, 5000]
    target = []
    purePriori = []
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
        result2 = list(pureApriori(transactions,min_support))
        end2 = time.time()
        purePriori.append(end2-start2)

        print('Backing up...')
        backup(fileBackup, numDatas, target, purePriori)
    labels = ['target', 'pureFuncion']
    labelD = {'y': 'Time (s)', 'x': 'Num Transactions'}
    backup(fileBackup,numDatas, target, purePriori)
    drawPlot(numDatas, [target ,purePriori], labels, labelD).savefig('compareNormal/reslut.png')
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
    # testSpeedWithLib()
    testSpeedWithLibMinSub()