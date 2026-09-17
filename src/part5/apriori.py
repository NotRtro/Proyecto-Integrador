import itertools
import math
from collections import Counter

def apriori(transactions,min_support=.02):
    baskets=[set(t) for t in transactions]; n=len(baskets)
    if not n: return {}
    minimum=math.ceil(min_support*n)
    counts=Counter(frozenset([v]) for basket in baskets for v in basket)
    level={k:v for k,v in counts.items() if v>=minimum}; all_sets=dict(level); size=2
    while level:
        previous=set(level); keys=list(previous)
        candidates={a|b for i,a in enumerate(keys) for b in keys[i+1:] if len(a|b)==size and all(frozenset(s) in previous for s in itertools.combinations(a|b,size-1))}
        level={c:sum(c<=b for b in baskets) for c in candidates}
        level={c:v for c,v in level.items() if v>=minimum}
        all_sets.update(level); size+=1
    return all_sets
