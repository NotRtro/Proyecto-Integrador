class DGIM:
    """At most two buckets per power of two; timestamps are bucket ends."""
    def __init__(self,window):
        if window<1: raise ValueError('Ventana debe ser positiva')
        self.window=window; self.time=0; self.buckets=[]
    def add(self,bit):
        self.time+=1
        self.buckets=[b for b in self.buckets if b[1]>self.time-self.window]
        if bit: self.buckets.insert(0,(1,self.time))
        size=1
        while True:
            ids=[i for i,b in enumerate(self.buckets) if b[0]==size]
            if len(ids)<=2: break
            newer,older=ids[-2:]
            merged=(size*2,self.buckets[newer][1])
            self.buckets[newer:older+1]=[merged]; size*=2
    def query(self):
        if not self.buckets: return 0.
        if self.time<=self.window: return float(sum(b[0] for b in self.buckets))
        return sum(b[0] for b in self.buckets)-self.buckets[-1][0]/2
