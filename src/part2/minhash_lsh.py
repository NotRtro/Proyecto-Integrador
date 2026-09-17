import hashlib
import random
PRIME=4294967311

def signature(tokens, hashes=128, seed=42):
    rng=random.Random(seed)
    coeffs=[(rng.randrange(1,PRIME),rng.randrange(PRIME)) for _ in range(hashes)]
    values=[int.from_bytes(hashlib.blake2b(t.encode(),digest_size=4).digest(),'little') for t in tokens]
    return [min(((a*x+b)%PRIME for x in values),default=PRIME) for a,b in coeffs]

def band_keys(row,bands,rows):
    idx,sig=row
    return [((b,tuple(sig[b*rows:(b+1)*rows])),idx) for b in range(bands)]
