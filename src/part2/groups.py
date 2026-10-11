"""Grupos de requerimientos casi idénticos y su etiqueta (Q3).

Motor: Spark para la frecuencia documental del corpus; el driver une los pares en grupos.

LSH entrega pares. Para leerlos como temas ("¿qué se repite?") los pares verificados
(Jaccard >= 0,7) se unen en grupos con componentes conexas (union-find sobre ~130 mil
aristas, en memoria). Los pares del mismo procedimiento (copias o ítems del mismo
proceso) no unen grupos.

Etiqueta de cada grupo: las palabras que mejor lo distinguen del resto del corpus,
puntaje = (fracción de procesos del grupo que la usan) × log(N / frecuencia documental),
como un TF-IDF por grupo. Se exige que la palabra aparezca en al menos la mitad del
grupo y se descartan números y stopwords de dominio.

Límite: el enlace simple encadena; dos procesos de un grupo pueden parecerse poco entre
sí si los une una cadena de pares parecidos. Por eso se reporta también el Jaccard medio
de las aristas del grupo.
"""

import math
from collections import Counter, defaultdict

import pandas as pd
from pyspark.sql import functions as F

MIN_GROUP = 3
LABEL_TERMS = 4
MIN_TERM_SHARE = 0.5
TOP_GROUPS = 200


def components(edges):
    """Union-find: {ocid: id del grupo} a partir de pares (left, right)."""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for left, right in edges:
        a, b = find(left), find(right)
        if a != b:
            parent[a] = b
    return {x: find(x) for x in parent}


def label(texts, document_frequency, documents, stopwords):
    """Etiqueta de un grupo: hasta LABEL_TERMS palabras con mayor puntaje TF-IDF de grupo."""
    counts = Counter(t for text in texts for t in set((text or '').split()))
    size = len(texts)
    scored = [
        (share * math.log(documents / document_frequency[term]), term)
        for term, n in counts.items()
        if (share := n / size) >= MIN_TERM_SHARE
        and not term.isdigit()
        and len(term) > 2
        and term not in stopwords
        and document_frequency.get(term)
    ]
    return ' · '.join(term for _, term in sorted(scored, reverse=True)[:LABEL_TERMS])


def build(pairs, features, stopwords):
    """Grupos con etiqueta y contexto. pairs: DataFrame de repeated.classify."""
    edges = pairs.filter(~F.col('same_procedure')).select('left', 'right', 'jaccard').toPandas()
    group_of = components(zip(edges.left, edges.right))
    members = defaultdict(list)
    for ocid, root in group_of.items():
        members[root].append(ocid)
    kept = {root: ocids for root, ocids in members.items() if len(ocids) >= MIN_GROUP}
    group_id = {ocid: root for root, ocids in kept.items() for ocid in ocids}

    spark = features.sparkSession
    membership = spark.createDataFrame(list(group_id.items()), 'ocid string, group_root string')
    context = (
        features.join(membership, 'ocid')
        .select(
            'ocid', 'group_root', 'normalized_text', 'buyer_id', 'category', 'modality', 'year', 'tender_amount_pen'
        )
        .toPandas()
    )
    names = (
        pairs.select(F.col('left').alias('ocid'), F.col('left_buyer_name').alias('buyer_name'))
        .unionByName(pairs.select(F.col('right').alias('ocid'), F.col('right_buyer_name').alias('buyer_name')))
        .dropDuplicates(['ocid'])
        .toPandas()
    )
    context = context.merge(names, on='ocid', how='left')

    terms = {t for text in context.normalized_text for t in set((text or '').split())}
    documents = features.count()
    tokens = features.select('ocid', F.explode(F.array_distinct(F.split('normalized_text', ' '))).alias('term'))
    wanted = spark.createDataFrame([(t,) for t in terms], 'term string')
    frequency = tokens.join(F.broadcast(wanted), 'term').groupBy('term').agg(F.count('*').alias('df')).toPandas()
    document_frequency = dict(zip(frequency.term, frequency.df))

    edges['group_root'] = edges.left.map(group_id)
    edge_stats = edges.dropna(subset=['group_root']).groupby('group_root').jaccard.agg(['mean', 'count'])
    rows = []
    for root, g in context.groupby('group_root'):
        top_buyer = g.buyer_name.value_counts()
        rows.append(
            dict(
                group_root=root,
                label=label(g.normalized_text.tolist(), document_frequency, documents, stopwords),
                processes=len(g),
                buyers=g.buyer_id.nunique(),
                top_buyer=top_buyer.index[0] if len(top_buyer) else None,
                top_buyer_share=float(top_buyer.iloc[0] / len(g)) if len(top_buyer) else None,
                category=g.category.mode().iloc[0] if g.category.notna().any() else None,
                modality=g.modality.mode().iloc[0] if g.modality.notna().any() else None,
                years=', '.join(str(y) for y in sorted(g.year.dropna().unique())),
                tender_amount_pen=float(g.tender_amount_pen.sum()),
                pairs=int(edge_stats.loc[root, 'count']) if root in edge_stats.index else 0,
                mean_jaccard=float(edge_stats.loc[root, 'mean']) if root in edge_stats.index else None,
                example=(g.normalized_text.iloc[0] or '')[:160],
            )
        )
    groups = pd.DataFrame(rows).sort_values(['processes', 'buyers'], ascending=False).reset_index(drop=True)
    groups.insert(0, 'group', range(1, len(groups) + 1))
    sizes = groups.processes
    summary = dict(
        groups=len(groups),
        processes_in_groups=int(sizes.sum()),
        largest=int(sizes.max()) if len(sizes) else 0,
        median_size=float(sizes.median()) if len(sizes) else 0,
        single_buyer_groups=int((groups.buyers == 1).sum()),
        multi_buyer_groups=int((groups.buyers > 1).sum()),
        size_distribution={
            label_: int(((sizes >= low) & (sizes <= high)).sum())
            for label_, low, high in [('3-5', 3, 5), ('6-20', 6, 20), ('21-100', 21, 100), ('101+', 101, 10**9)]
        },
        min_group=MIN_GROUP,
        method='Componentes conexas de los pares con Jaccard >= 0,7 (sin pares del mismo procedimiento); etiqueta = '
        'palabras con mayor (fracción del grupo) × log(N / df), presentes en al menos la mitad del grupo.',
    )
    return groups.drop(columns='group_root').head(TOP_GROUPS), summary
