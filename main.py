from apriori import apriori as targetApriori
from apyori import apriori as pyLibOptimizedApriori
from utils import generateData
import time
import json


def main():
    with open('transactions.json','rb') as f:
        rTransactions = json.load(f)
    with open('items.json','rb') as f:
        items = json.load(f)
    
    gTransactions = generateData(items,5000)
    transactions = rTransactions

    min_support = 0.02
    min_confidence = 0.1
    min_lift = 0.0

    start = time.time()
    results2 = list(targetApriori(transactions=transactions, items=items, min_confidence=min_confidence,min_support=min_support,min_lift=min_lift,numReduce=1))
    t2 = time.time()-start
    print(len(results2))
    print(t2)
    print()
    for i in results2[:5]:
        print(i)


    start = time.time()
    results1 = list(pyLibOptimizedApriori(transactions,min_confidence=min_confidence,min_support=min_support,min_lift=min_lift))
    t1 = time.time()-start
    print(len(results1))
    print(t1)
    for i in results1[:5]:
        print(i)

    print(t1/t2)

if __name__ == '__main__':
    main()
