import math
import mmh3
from bitarray import bitarray
from functools import reduce


class BloomFilter(object):
    def __init__(self, itemsCounnt, fpProb):
        self.fpProb = fpProb
        self.buffer = set()
        self.itemsCounnt = itemsCounnt
        self.size = self.getSize(itemsCounnt, fpProb)
        self.hashCount = self.getHashCount(self.size, itemsCounnt)
        self.bitArray = bitarray(self.size)
        self.listHashFunc = list(map(lambda i: lambda item: (mmh3.hash_from_buffer(item, i) % self.size),range(self.hashCount)))
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

