from itertools import combinations
from utils import generateData, drawPlot, backup, loadBaseDataSet

def Apriori_gen(prev_candidates, length):
    item_set = set()
    for candidate in prev_candidates:
        for item in candidate:
            item_set.add(item)
    items = sorted(item_set)
    tmp_next_candidates = list(map(lambda x: frozenset(x),combinations(items, length)))
    if length < 3:
        return list(tmp_next_candidates)
    next_candidates = [
        candidate for candidate in tmp_next_candidates
        if all(
            True if frozenset(x) in prev_candidates else False
            for x in combinations(candidate, length - 1))
    ]

    return next_candidates


def Apriori_prune(Ck,minsupport):
    L = []
    for i in Ck:
        if Ck[i] >= minsupport:
            L.append(i)
    return sorted(L)


def Apriori_count_subset(Canditate,Canditate_len,transactions):
    Lk = dict()
    for transaction in transactions:
        for key in Canditate:
            keyStr = frozenset(key)
            if keyStr not in Lk:
                Lk[keyStr] = 0
            if not False in list(map(lambda x: x in transaction, key)):
                Lk[keyStr] +=1
    return Lk

def apriori(transactions,minSupport):
    result = []
    minsupport = minSupport*len(transactions)
    C1={} 
    for transaction in transactions:
        for item in transaction:
            itemStr = frozenset([item])
            if itemStr in C1:
                C1[itemStr] +=1
            else:
                C1[itemStr] = 1

    L = []
    L1 = Apriori_prune(C1,minsupport)
    L = Apriori_gen(L1,2)
    result += L1
    k=2
    while L != []:
        C = dict()
        C = Apriori_count_subset(L,len(L),transactions)
        fruquent_itemset = []
        fruquent_itemset = Apriori_prune(C,minsupport)
        result += fruquent_itemset
        L = Apriori_gen(fruquent_itemset,k+1)
        k += 1
    return result