import hashlib
import math

def hashes(value, count, modulus):
    b=str(value).encode()
    a=int.from_bytes(hashlib.blake2b(b,digest_size=8,person=b'oece-a').digest(),'little')
    c=int.from_bytes(hashlib.blake2b(b,digest_size=8,person=b'oece-b').digest(),'little')|1
    return [(a+i*c+i*i)%modulus for i in range(count)]

class BloomFilter:
    def __init__(self,bits,hash_count):
        if bits<1 or hash_count<1: raise ValueError('Parámetros positivos requeridos')
        self.bits,self.hash_count=bits,hash_count
        self.data=bytearray((bits+7)//8)
    def add(self,value):
        for p in hashes(value,self.hash_count,self.bits): self.data[p//8]|=1<<(p%8)
    def __contains__(self,value):
        return all(self.data[p//8]&(1<<(p%8)) for p in hashes(value,self.hash_count,self.bits))
    def theoretical_fpr(self,n):
        return (1-math.exp(-self.hash_count*n/self.bits))**self.hash_count
