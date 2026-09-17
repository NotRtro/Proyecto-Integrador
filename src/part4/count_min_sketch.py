from array import array
import hashlib

class CountMinSketch:
    def __init__(self,width,depth):
        if width<1 or depth<1: raise ValueError('Parámetros positivos requeridos')
        self.width,self.depth=width,depth
        self.data=[array('Q',[0])*width for _ in range(depth)]
    def _positions(self,value):
        return [int.from_bytes(hashlib.blake2b(str(value).encode(),digest_size=8,person=f'cms{i}'.encode()).digest(),'little')%self.width for i in range(self.depth)]
    def add(self,value):
        for row,p in zip(self.data,self._positions(value)): row[p]+=1
    def query(self,value):
        return min(row[p] for row,p in zip(self.data,self._positions(value)))
    @property
    def bytes(self): return self.width*self.depth*8
