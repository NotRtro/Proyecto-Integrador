"""Frecuencia de ítems y participación de proveedores: quién participa, quién gana y quién nunca gana.

Motor: Spark (agregaciones sobre las tablas items, tenderers y suppliers).

Ítems. Cada ítem trae un código del catálogo CUBSO del OECE (16 dígitos): los 8 primeros
son el código UNSPSC del bien o servicio y los 2 primeros, su segmento (combustibles,
medicamentos, construcción...). Se cuentan ítems, procesos, entidades y monto por
segmento y por código.

Participación. `tender.tenderers` lista a los participantes registrados de cada proceso,
y todo proveedor adjudicado figura entre ellos (verificado en los datos). Por proveedor:
- participaciones: procesos en los que se registró;
- victorias: procesos en los que recibió al menos una adjudicación;
- tasa de victoria = victorias / participaciones.

Lectura: "participante" no es lo mismo que "oferta presentada" (inscribirse es gratuito
y muchos lo hacen solo para ver las bases), así que una tasa baja no significa que el
proveedor pierda en la evaluación. Las personas naturales (RUC 10, contiene el DNI) no
aparecen con nombre en los rankings: se reportan agregadas.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

# Segmentos UNSPSC (primeros dos dígitos del código), títulos abreviados.
SEGMENTS = {
    '10': 'Material vivo vegetal y animal',
    '11': 'Minerales y textiles sin procesar',
    '12': 'Productos químicos',
    '13': 'Resinas, cauchos y elastómeros',
    '14': 'Papel y derivados',
    '15': 'Combustibles y lubricantes',
    '20': 'Maquinaria de minería y perforación',
    '21': 'Maquinaria agrícola y forestal',
    '22': 'Maquinaria de construcción',
    '23': 'Maquinaria industrial',
    '24': 'Manejo y almacenamiento de materiales',
    '25': 'Vehículos y sus accesorios',
    '26': 'Generación y distribución de energía',
    '27': 'Herramientas y maquinaria general',
    '30': 'Materiales de construcción y estructuras',
    '31': 'Componentes de manufactura',
    '32': 'Componentes electrónicos',
    '39': 'Iluminación y material eléctrico',
    '40': 'Fluidos, climatización y tuberías',
    '41': 'Equipo de laboratorio y medición',
    '42': 'Equipo y suministros médicos',
    '43': 'Tecnologías de la información y telecomunicaciones',
    '44': 'Equipos y útiles de oficina',
    '45': 'Impresión, fotografía y audiovisuales',
    '46': 'Defensa, orden público y seguridad',
    '47': 'Limpieza y su equipo',
    '48': 'Equipos de servicio de alimentos',
    '49': 'Deportes y recreación',
    '50': 'Alimentos, bebidas y tabaco',
    '51': 'Medicamentos y productos farmacéuticos',
    '52': 'Electrodomésticos y artículos para el hogar',
    '53': 'Ropa, calzado y artículos personales',
    '54': 'Relojes y joyería',
    '55': 'Publicaciones impresas y electrónicas',
    '56': 'Muebles y mobiliario',
    '60': 'Material educativo y musical',
    '64': 'Instrumentos financieros',
    '70': 'Servicios agrícolas, pesqueros y forestales',
    '71': 'Servicios de minería, petróleo y gas',
    '72': 'Servicios de construcción y mantenimiento',
    '73': 'Servicios de producción industrial',
    '76': 'Servicios de limpieza industrial',
    '77': 'Servicios medioambientales',
    '78': 'Transporte, almacenaje y correo',
    '80': 'Servicios de gestión, empresariales y administrativos',
    '81': 'Servicios de ingeniería, investigación y tecnología',
    '82': 'Servicios editoriales, de diseño y publicidad',
    '83': 'Servicios públicos (agua, energía, telecomunicaciones)',
    '84': 'Servicios financieros y de seguros',
    '85': 'Servicios de salud',
    '86': 'Servicios educativos y de formación',
    '90': 'Servicios de viajes, alimentación y alojamiento',
    '91': 'Servicios personales y domésticos',
    '92': 'Servicios de defensa, orden público y seguridad',
    '93': 'Servicios políticos y cívicos',
    '94': 'Organizaciones y clubes',
    '95': 'Terrenos, edificios y estructuras',
}
TOP = 25
# Para "participa y nunca gana" se exige un mínimo de participaciones: con una o dos,
# no ganar es lo normal y no dice nada.
NEVER_WON_MIN = 20
PARTICIPATION_BINS = [(1, 1), (2, 4), (5, 19), (20, 99), (100, None)]
NAMED_TYPES = ['persona_juridica', 'consorcio', 'extranjero', 'otro']


def segment_names(spark):
    return spark.createDataFrame(list(SEGMENTS.items()), 'segment string, segment_name string')


def item_frequency(spark, items):
    """Ítems por segmento UNSPSC y por código CUBSO (con su descripción más frecuente)."""
    coded = items.filter(F.col('classification').isNotNull()).withColumn('segment', F.substring('classification', 1, 2))
    by_segment = (
        coded.groupBy('segment')
        .agg(
            F.count('*').alias('items'),
            F.countDistinct('ocid').alias('processes'),
            F.sum('amount_pen').alias('amount_pen'),
        )
        .join(segment_names(spark), 'segment', 'left')
        .withColumn('segment_name', F.coalesce('segment_name', F.concat(F.lit('Segmento '), 'segment')))
        .withColumn('share_items', F.col('items') / F.sum('items').over(Window.partitionBy()))
        .orderBy(F.desc('items'))
    )
    description = (
        coded.groupBy('classification', 'classification_description')
        .count()
        .withColumn(
            '_rn',
            F.row_number().over(Window.partitionBy('classification').orderBy(F.desc('count'))),
        )
        .filter('_rn = 1')
        .select('classification', F.col('classification_description').alias('description'))
    )
    by_code = (
        coded.groupBy('classification', 'segment')
        .agg(
            F.count('*').alias('items'),
            F.countDistinct('ocid').alias('processes'),
            F.sum('amount_pen').alias('amount_pen'),
        )
        .join(description, 'classification', 'left')
        .join(segment_names(spark), 'segment', 'left')
        .orderBy(F.desc('items'))
        .limit(TOP * 2)
    )
    by_year = (
        coded.groupBy('year', 'segment').agg(F.count('*').alias('items')).join(segment_names(spark), 'segment', 'left')
    )
    total = items.count()
    summary = dict(
        items=total,
        coded_items=coded.count(),
        distinct_codes=coded.select('classification').distinct().count(),
        distinct_commodities=coded.select(F.substring('classification', 1, 8)).distinct().count(),
        processes_with_items=items.select('ocid').distinct().count(),
    )
    return by_segment, by_code, by_year, summary


def participation(tenderers, suppliers):
    """Participaciones, victorias y monto ganado por proveedor."""
    entered = tenderers.groupBy('tenderer_id').agg(
        F.countDistinct('ocid').alias('participations'),
        F.countDistinct('buyer_id').alias('buyers_entered'),
        F.first('tenderer_name').alias('name'),
        F.first('tenderer_type').alias('type'),
    )
    won = suppliers.groupBy('supplier_id').agg(
        F.countDistinct('ocid').alias('wins'), F.sum('allocated_amount_pen').alias('amount_won_pen')
    )
    return (
        entered.join(won, entered.tenderer_id == won.supplier_id, 'left')
        .drop('supplier_id')
        .withColumn('wins', F.coalesce('wins', F.lit(0)))
        .withColumn('amount_won_pen', F.coalesce('amount_won_pen', F.lit(0.0)))
        .withColumn('win_rate', F.col('wins') / F.col('participations'))
        .withColumnRenamed('tenderer_id', 'id')
    )


def participation_rankings(stats):
    """Rankings con nombre (sin personas naturales): más participan, más ganan y nunca ganan."""
    named = stats.filter(F.col('type').isin(NAMED_TYPES))
    columns = ['id', 'name', 'type', 'participations', 'wins', 'win_rate', 'amount_won_pen', 'buyers_entered']
    rankings = [
        ('mas_participan', named.orderBy(F.desc('participations'), F.desc('wins'))),
        ('mas_ganan', named.orderBy(F.desc('wins'), F.desc('participations'))),
        (
            'nunca_ganan',
            named.filter((F.col('wins') == 0) & (F.col('participations') >= NEVER_WON_MIN)).orderBy(
                F.desc('participations')
            ),
        ),
    ]
    frames = [df.select(*columns).limit(TOP).withColumn('ranking', F.lit(name)) for name, df in rankings]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def participation_bins(stats):
    """Proveedores por tramo de participaciones: cuántos nunca ganan y su tasa de victoria."""
    bucket = F.lit(None).cast('string')
    for low, high in reversed(PARTICIPATION_BINS):
        label = f'{low}' if low == high else (f'{low}+' if high is None else f'{low}-{high}')
        condition = F.col('participations') >= low
        if high is not None:
            condition = condition & (F.col('participations') <= high)
        bucket = F.when(condition, label).otherwise(bucket)
    return (
        stats.withColumn('bin', bucket)
        .groupBy('bin')
        .agg(
            F.count('*').alias('providers'),
            F.sum((F.col('wins') == 0).cast('int')).alias('never_won'),
            F.sum('participations').alias('participations'),
            F.sum('wins').alias('wins'),
            F.min('participations').alias('min_participations'),
        )
        .withColumn('never_won_share', F.col('never_won') / F.col('providers'))
        .withColumn('win_rate', F.col('wins') / F.col('participations'))
        .orderBy('min_participations')
        .drop('min_participations')
    )


def participation_summary(stats, tenderers):
    s = stats.agg(
        F.count('*').alias('providers'),
        F.sum((F.col('wins') == 0).cast('int')).alias('never_won'),
        F.sum('participations').alias('participations'),
        F.sum('wins').alias('wins'),
        F.sum(((F.col('wins') == 0) & (F.col('participations') >= NEVER_WON_MIN)).cast('int')).alias(
            'never_won_frequent'
        ),
    ).first()
    by_type = {
        r['type']: dict(
            providers=r['providers'], participations=r['participations'], wins=r['wins'], never_won=r['never_won']
        )
        for r in stats.groupBy('type')
        .agg(
            F.count('*').alias('providers'),
            F.sum('participations').alias('participations'),
            F.sum('wins').alias('wins'),
            F.sum((F.col('wins') == 0).cast('int')).alias('never_won'),
        )
        .collect()
    }
    top = stats.orderBy(F.desc('participations')).limit(100).agg(F.sum('participations').alias('p')).first()['p']
    return dict(
        processes_with_tenderers=tenderers.select('ocid').distinct().count(),
        providers=s['providers'],
        never_won=s['never_won'],
        never_won_share=s['never_won'] / s['providers'],
        never_won_frequent=s['never_won_frequent'],
        never_won_min=NEVER_WON_MIN,
        participations=s['participations'],
        wins=s['wins'],
        overall_win_rate=s['wins'] / s['participations'],
        top100_participation_share=top / s['participations'],
        by_type=by_type,
        note='Participante = registrado en el proceso (tender.tenderers), no necesariamente con oferta presentada. '
        'Las personas naturales no aparecen con nombre en los rankings.',
    )
