import re
import unicodedata
STOP = set('de del la las el los y o en para por con un una al a se que su'.split())


def normalize_text(text):
    text = unicodedata.normalize('NFKD', text or '').encode('ascii', 'ignore').decode().lower()
    return ' '.join(t for t in re.findall(r'[a-z0-9]+', text) if t not in STOP and len(t) > 1)


def shingles(text, width=2):
    tokens = normalize_text(text).split()
    return sorted(set(' '.join(tokens[i:i+width]) for i in range(max(0, len(tokens)-width+1)))) or sorted(set(tokens))
