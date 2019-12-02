import math
import mmh3
from bitarray import bitarray
from itertools import combinations
from functools import reduce


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


class BloomFilter(object):
    def __init__(self, itemsCounnt, fpProb):
        # False posible probability in decimal
        self.fpProb = fpProb
        self.buffer = set()
        self.itemsCounnt = itemsCounnt
        # Size of bit array to use
        self.size = self.getSize(itemsCounnt, fpProb)
        # number of hash functions to use
        self.hashCount = self.getHashCount(self.size, itemsCounnt)
        # Bit array of given size
        self.bitArray = bitarray(self.size)
        self.listHashFunc = list(map(lambda i: lambda item: (mmh3.hash_from_buffer(item, i) % self.size),range(self.hashCount)))
        # initialize all bits as 0
        self.bitArray.setall(0)

    def setBitArray(self, bitarr):
        self.bitArray = bitarr

    def getNummElement(self):
        log_cal = math.log(1-self.bitArray.count(1)/self.size)
        return -(self.size * log_cal)/self.hashCount

    def getUnionWithOne(self, bl):
        if self.itemsCounnt == bl.itemsCounnt and self.fpProb == bl.fpProb:
            union = BloomFilter(self.itemsCounnt, self.fpProb)
            union.setBitArray(self.bitArray | bl.bitArray)
            return union
        return None

    def getUnionWithMany(self, bls):
        return reduce(lambda a, b: b.getUnionWithOne(a), bls, self)

    def add(self, item):
        digests = []
        for i in range(self.hashCount):
            digest = mmh3.hash_from_buffer(item, i) % self.size
            digests.append(digest)
            self.bit_array[digest] = True

    def addHashed(self, itemsHashed):
        for digest in itemsHashed:
            self.bitArray[digest] = True

    def check(self, item):
        for i in range(self.hashCount):
            digest = mmh3.hash_from_buffer(item, i) % self.size
            if not self.bitArray[digest]:
                return False
        return True

    @classmethod
    def getSize(self, n, p):
        m = -(n * math.log(p))/(math.log(2)**2)
        return int(m)

    @classmethod
    def getHashCount(self, m, n):
        k = (m/n) * math.log(2)
        return int(k)


if __name__ == '__main__':
    # X = [1,2,3,4,5,15]
    a = BloomFilter(10000, 0.05)
    listHashfunc = a.listHashFunc
    X = [1,2,3]
    
    XHased = list(map(lambda x: list(map(lambda y: y(str(x)), listHashfunc)),range(10)))
    print(XHased)
    # for i in X:
    #     a.add(str(i))
    # X = [1,3,2,9,8,10,15]
    # b = BloomFilter(10000, 0.05)
    # for i in X:
    #     b.add(str(i))
    
    # X = [1,3,2,11,12,10]
    # c = BloomFilter(10000, 0.05)
    # for i in X:
    #     c.add(str(i))

    # print(getNumIntersectionWith([a,b]))

    # bls = ['a','b','c','d']
    # print(getNumIntersectionWith(bls))
    # print(getNumIntersectionWith(bls)[-1])

    # print(round(a.getNummElement()))
    # print(round(b.getNummElement()))
    # print(round(a.getNumIntersectionWith(b)))
