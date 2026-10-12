"""Gráficos del README a partir de los artefactos del pipeline (artifacts/).

Uso:  make figures   (dentro del contenedor del pipeline; escribe assets/readme/*.png)

No recalcula nada: lee los mismos CSV y JSON que muestra la web, así que las figuras
del README coinciden con las del reporte interactivo y con notebooks/sustento.ipynb.
"""

import json
from pathlib import Path

import matplotlib
import matplotlib.dates

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
ART = next(p for p in [ROOT / 'artifacts', Path('/artifacts')] if p.is_dir())
OUT = ROOT / 'assets' / 'readme'
TEAL, ORANGE, NAVY, BROWN, GREY = '#167c80', '#de7c45', '#384b72', '#93785e', '#8a96a3'
plt.rcParams.update(
    {
        'figure.dpi': 110,
        'savefig.dpi': 130,
        'axes.grid': True,
        'grid.alpha': 0.3,
        'axes.spines.top': False,
        'axes.spines.right': False,
        'font.size': 10,
        'axes.titlesize': 11,
        'axes.titleweight': 'bold',
    }
)


def csv(name):
    return pd.read_csv(ART / name)


def doc(name):
    return json.loads((ART / name).read_text(encoding='utf-8'))


def save(fig, name):
    fig.tight_layout()
    fig.savefig(OUT / f'{name}.png', bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print('  ', name)


def miles(v, _=None):
    return f'{v:,.0f}'.replace(',', ' ')


def kdd_funnel():
    q = doc('metadata/quality_summary.json')
    steps = {
        'Registros OCDS': q['raw_records'],
        'Procesos únicos (ocid)': q['raw_records'] - q['duplicate_ocids'] - q['missing_ocid'],
        'Convocados 2023–2025': q['selected_processes'],
    }
    tables = q['table_counts']
    fig, axes = plt.subplots(1, 2, figsize=(13, 3.6))
    ax = axes[0]
    ax.barh(list(steps)[::-1], list(steps.values())[::-1], color=[ORANGE, TEAL, NAVY])
    for i, v in enumerate(list(steps.values())[::-1]):
        ax.text(v, i, f' {miles(v)}', va='center')
    ax.set_title('Del registro a los procesos analizados'), ax.xaxis.set_major_formatter(miles)
    ax = axes[1]
    names = list(tables)
    ax.bar(names, [tables[n] for n in names], color=TEAL)
    for i, n in enumerate(names):
        ax.text(i, tables[n], miles(tables[n]), ha='center', va='bottom', fontsize=8)
    ax.set_title('Filas de cada tabla Parquet'), ax.yaxis.set_major_formatter(miles)
    save(fig, '01_kdd')


def monthly_and_amounts():
    monthly = csv('eda/monthly_evolution.csv').sort_values(['year', 'month'])
    hist = csv('eda/amount_histogram.csv')
    fig, axes = plt.subplots(1, 2, figsize=(13, 3.8))
    for (year, g), color in zip(monthly.groupby('year'), [NAVY, TEAL, ORANGE]):
        axes[0].plot(g.month, g.processes, marker='o', color=color, label=str(year))
    axes[0].set_xticks(range(1, 13)), axes[0].legend(), axes[0].set_title('Procesos convocados por mes')
    axes[0].yaxis.set_major_formatter(miles)
    names = {'goods': 'Bienes', 'services': 'Servicios', 'works': 'Obras'}
    for (cat, g), color in zip(hist.groupby('category'), [TEAL, ORANGE, NAVY]):
        g = g.groupby('log10_bin').processes.sum()
        axes[1].plot(10**g.index, g.values, color=color, label=names.get(cat, cat))
    axes[1].set_xscale('log'), axes[1].set_xlim(1e3, 2e9), axes[1].legend()
    axes[1].set_title('Monto convocado por proceso (escala logarítmica)'), axes[1].set_xlabel('S/')
    save(fig, '02_tiempo_montos')


def items():
    segments = csv('eda/item_segments.csv')
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.6))
    top = segments.head(10).iloc[::-1]
    axes[0].barh(top.segment_name.str[:42], top['items'], color=TEAL)
    axes[0].set_title('Segmentos UNSPSC con más ítems'), axes[0].xaxis.set_major_formatter(miles)
    by_amount = segments.sort_values('amount_pen', ascending=False).head(10).iloc[::-1]
    axes[1].barh(by_amount.segment_name.str[:42], by_amount.amount_pen / 1e9, color=ORANGE)
    axes[1].set_title('Segmentos con más monto (miles de millones de S/)')
    save(fig, '03_items')


def participation():
    ranks = csv('eda/participation_rankings.csv')
    bins = csv('eda/participation_bins.csv')
    s = doc('eda/participation_summary.json')
    fig, axes = plt.subplots(1, 3, figsize=(17, 4.8), gridspec_kw={'width_ratios': [1.25, 1.25, 1]})
    for ax, (key, title) in zip(
        axes[:2], [('mas_participan', 'Los que más participan'), ('mas_ganan', 'Los que más ganan')]
    ):
        r = ranks[ranks.ranking == key].head(10).iloc[::-1]
        label = r.name.str.replace(r'\s+SOCIEDAD.*$', '', regex=True).str[:28]
        ax.barh(label, r.wins, color=TEAL, label='Ganó')
        ax.barh(label, r.participations - r.wins, left=r.wins, color=ORANGE, label='No ganó')
        ax.set_title(title), ax.tick_params(axis='y', labelsize=8), ax.xaxis.set_major_formatter(miles)
    axes[0].legend(loc='lower right')
    x = np.arange(len(bins))
    axes[2].bar(x, bins.providers - bins.never_won, color=TEAL, label='Ganó alguna vez')
    axes[2].bar(x, bins.never_won, bottom=bins.providers - bins.never_won, color=ORANGE, label='Nunca ganó')
    axes[2].set_xticks(x, bins['bin']), axes[2].set_xlabel('participaciones por proveedor')
    axes[2].set_title(f"Nunca ganaron: {s['never_won_share']:.0%}"), axes[2].legend()
    axes[2].yaxis.set_major_formatter(miles)
    save(fig, '04_participacion')


def spark():
    bench = csv('eda/rdd_dataframe_benchmark.csv')
    svp = csv('eda/spark_vs_pandas.csv')
    part = doc('eda/partitioning_experiment.json')
    fig, axes = plt.subplots(1, 3, figsize=(16, 3.8))
    pivot = bench.pivot(index='description', columns='engine', values='median_seconds')
    pivot.index = pivot.index.str[:34]
    pivot[['DataFrame', 'RDD']].plot.barh(ax=axes[0], color=[TEAL, ORANGE])
    axes[0].set_title('RDD frente a DataFrame (mediana, s)'), axes[0].set_ylabel('')
    shuffles = pd.DataFrame(part['shuffle_partitions'])
    for adaptive, color, label in [(False, ORANGE, 'sin AQE'), (True, TEAL, 'con AQE')]:
        g = shuffles[shuffles.adaptive == adaptive]
        axes[1].plot(g.shuffle_partitions, g.seconds, marker='o', color=color, label=label)
    axes[1].set_xscale('log'), axes[1].legend(), axes[1].set_xlabel('spark.sql.shuffle.partitions')
    axes[1].set_title('Particiones del shuffle (s)')
    axes[2].plot(svp.processes, svp.spark_seconds, marker='o', color=ORANGE, label='Spark')
    axes[2].plot(svp.processes, svp.pandas_seconds, marker='o', color=TEAL, label='pandas')
    axes[2].set_title('Misma agregación: Spark frente a pandas (s)'), axes[2].legend()
    axes[2].xaxis.set_major_formatter(miles)
    save(fig, '05_spark')


def concentration():
    hhi = csv('eda/buyer_hhi_summary.csv')
    effect = csv('eda/sampling_effect.csv')
    fig, axes = plt.subplots(1, 3, figsize=(16, 3.8))
    names = {'local': 'Local', 'regional': 'Regional', 'nacional': 'Nacional', 'empresa_publica': 'Empresa pública'}
    for level, color in zip(names, [ORANGE, TEAL, NAVY, BROWN]):
        g = hhi[hhi.buyer_level == level].sort_values('year')
        axes[0].plot(g.year.astype(str), g.median_hhi, marker='o', color=color, label=names[level])
    axes[0].axhline(2500, ls='--', c=GREY), axes[0].legend(fontsize=8)
    axes[0].set_title('HHI mediano de proveedores por entidad')
    year = effect.year.max()
    e = effect[effect.year == year]
    full = e[e.design == 'todos'].iloc[0]
    for design, color, label in [('aleatorio', ORANGE, 'procesos al azar'), ('conglomerados', TEAL, 'entidades')]:
        g = e[e.design == design].sort_values('fraction')
        axes[1].plot(g.fraction * 100, g.median_hhi, marker='o', color=color, label=label)
        axes[2].plot(g.fraction * 100, g.high_recall * 100, marker='o', color=color, label=label)
    axes[1].axhline(full.median_hhi, ls='--', c=GREY, label='todos los datos')
    axes[1].set_title(f'Muestrear sesga el HHI ({year})'), axes[1].set_xlabel('% de la muestra'), axes[1].legend()
    axes[2].set_title('% de entidades concentradas detectadas'), axes[2].set_xlabel('% de la muestra')
    axes[2].legend()
    save(fig, '06_concentracion')


def competition():
    adjusted = csv('eda/single_bidder_adjusted.csv')
    m = adjusted[adjusted.level == 'modalidad'].sort_values('raw_rate')
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.4), gridspec_kw={'width_ratios': [1.4, 1]})
    y = np.arange(len(m))
    axes[0].barh(y - 0.2, m.raw_rate * 100, height=0.4, color=ORANGE, label='observada')
    axes[0].barh(y + 0.2, m.adjusted_rate * 100, height=0.4, color=TEAL, label='ajustada por estratos')
    axes[0].set_yticks(y, m.name.str[:34]), axes[0].legend(loc='lower right')
    axes[0].set_title('% de procesos con un solo participante, por modalidad')
    b = csv('eda/bidders_by_modality.csv').sort_values('median')
    axes[1].barh(b.modality.str[:34], b['median'], color=NAVY)
    axes[1].set_title('Mediana de "postores" registrados por modalidad')
    save(fig, '07_competencia')


def similarity():
    err = csv('similarity/minhash_error.csv')
    curve = csv('similarity/s_curve_observed.csv')
    stab = csv('similarity/lsh_stability.csv')
    fig, axes = plt.subplots(1, 3, figsize=(16, 3.9))
    axes[0].plot(err.hashes, err.rmse_similar, 'o-', color=TEAL, label='observado')
    axes[0].plot(err.hashes, err.rmse_theoretical_similar, 's--', color=ORANGE, label='teórico √(J(1−J)/k)')
    axes[0].set_xscale('log', base=2), axes[0].set_xlabel('k'), axes[0].legend()
    axes[0].set_title('Error de MinHash frente al Jaccard exacto')
    c = curve[(curve.hashes == 128) & (curve.bands == 16) & (curve.rows == 8)]
    axes[1].plot(c.similarity, c.theoretical, color=ORANGE, label='1 − (1 − sʳ)ᵇ')
    axes[1].scatter(c.similarity, c.observed, s=np.clip(np.sqrt(c.pairs), 10, 160), color=TEAL, label='observada')
    axes[1].axvline(0.7, ls='--', c=GREY), axes[1].legend(), axes[1].set_xlabel('Jaccard')
    axes[1].set_title('Curva S de LSH (b = 16, r = 8)')
    s = stab.sort_values('f1_mean', ascending=False).head(10).iloc[::-1]
    axes[2].barh(s.config, s.f1_mean, xerr=[s.f1_mean - s.f1_min, s.f1_max - s.f1_mean], color=NAVY, capsize=3)
    axes[2].set_title('F1 de LSH en 5 muestras (media, mín–máx)'), axes[2].tick_params(axis='y', labelsize=8)
    save(fig, '08_similitud')


def lsh_groups():
    groups = csv('similarity/lsh_groups.csv')
    single = groups[groups.buyers == 1].head(8)
    multi = groups[groups.buyers > 1].head(8)
    fig, axes = plt.subplots(1, 2, figsize=(15, 3.9))
    for ax, g, color, title in [
        (axes[0], single, ORANGE, 'Grupos de una sola entidad'),
        (axes[1], multi, TEAL, 'Grupos de varias entidades'),
    ]:
        g = g.iloc[::-1].reset_index(drop=True)
        # Posiciones numéricas: dos grupos pueden compartir etiqueta.
        y = np.arange(len(g))
        ax.barh(y, g.processes, color=color)
        ax.set_yticks(y, g.label.fillna('—').str[:45])
        for i, (n, b) in enumerate(zip(g.processes, g.buyers)):
            ax.text(n, i, f" {n} procesos · {b} {'entidad' if b == 1 else 'entidades'}", va='center', fontsize=8)
        ax.set_title(title), ax.set_xlim(0, g.processes.max() * 1.7)
    save(fig, '09_grupos_lsh')


def ann():
    hnsw, ivf, scaling = csv('ann/hnsw_grid.csv'), csv('ann/ivf_grid.csv'), csv('ann/scaling.csv')
    fig, axes = plt.subplots(1, 2, figsize=(13, 3.9))
    axes[0].scatter(hnsw.mean_ms, hnsw.recall_at_10, color=TEAL, label='HNSW')
    axes[0].scatter(ivf.mean_ms, ivf.recall_at_10, color=ORANGE, label='IVF')
    axes[0].axvline(hnsw.exact_mean_ms.iloc[0], ls='--', c=GREY, label='búsqueda exacta')
    axes[0].set_xscale('log'), axes[0].set_xlabel('ms por consulta'), axes[0].set_ylabel('recall@10')
    axes[0].legend(), axes[0].set_title('Recall frente a latencia (196 mil procesos)')
    axes[1].plot(scaling.corpus_size, scaling.exact_mean_ms, 'o-', color=NAVY, label='exacta')
    axes[1].plot(scaling.corpus_size, scaling.hnsw_mean_ms, 'o-', color=TEAL, label='HNSW')
    axes[1].plot(scaling.corpus_size, scaling.ivf_mean_ms, 'o-', color=ORANGE, label='IVF')
    axes[1].set_xlabel('procesos en el corpus'), axes[1].set_ylabel('ms por consulta'), axes[1].legend()
    axes[1].set_title('La búsqueda exacta crece con N; HNSW casi no'), axes[1].xaxis.set_major_formatter(miles)
    save(fig, '10_ann')


def streams():
    bloom, cms = csv('streaming/bloom_grid.csv'), csv('streaming/cms_grid.csv')
    replay = csv('streaming/replay_batches.csv')
    fig, axes = plt.subplots(1, 3, figsize=(17, 4))
    axes[0].plot([1e-5, 1], [1e-5, 1], ls='--', c=GREY)
    axes[0].scatter(bloom.theoretical_fpr, bloom.final_fpr, color=TEAL, label='filtro lleno (sondas)')
    axes[0].scatter(bloom.expected_stream_fpr, bloom.observed_fpr, color=ORANGE, label='durante el flujo')
    axes[0].set_xscale('log'), axes[0].set_yscale('log'), axes[0].legend(fontsize=8)
    axes[0].set_xlabel('FPR teórico'), axes[0].set_ylabel('FPR observado'), axes[0].set_title('Bloom Filter')
    for domain, color in [('proveedor', NAVY), ('unspsc', BROWN)]:
        g = cms[(cms.domain == domain) & (cms.depth == 5)]
        axes[1].plot(g.width, g.heavy_mean_relative_error, 'o-', color=color, label=f'{domain}: 100 más frecuentes')
        axes[1].plot(g.width, g.tail_mean_relative_error, 's--', color=color, label=f'{domain}: cola')
    axes[1].set_xscale('log', base=2), axes[1].set_yscale('log'), axes[1].legend(fontsize=7)
    axes[1].set_xlabel('ancho w'), axes[1].set_title('Count-Min: error relativo medio')
    weeks = pd.to_datetime(replay.week_start)
    axes[2].plot(weeks, replay.dgim_exact, color=NAVY, label='exacto')
    axes[2].plot(weeks, replay.dgim_estimate, color=ORANGE, label='DGIM')
    axes[2].legend(), axes[2].set_title('Structured Streaming: postor único en 30 días')
    axes[2].xaxis.set_major_locator(matplotlib.dates.YearLocator())
    axes[2].xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%Y'))
    save(fig, '11_flujos')


def rules():
    r = csv('association/rules.csv')
    r = r[r.year == 'Todos']
    single = csv('association/single_bidder_rules.csv')
    single = single[single.consequent == 'POSTORES=uno'].head(8).iloc[::-1]
    fig, axes = plt.subplots(1, 2, figsize=(15, 4.2), gridspec_kw={'width_ratios': [1, 1.3]})
    for kind, color in [('informativa', TEAL), ('normativa', ORANGE), ('datos faltantes', GREY)]:
        g = r[r.kind == kind]
        axes[0].scatter(g.support, g.confidence, s=g.lift * 6, alpha=0.5, color=color, label=f'{kind} ({len(g)})')
    axes[0].set_xscale('log'), axes[0].set_xlabel('soporte'), axes[0].set_ylabel('confianza'), axes[0].legend()
    axes[0].set_title('Reglas FP-Growth (tamaño = lift)')
    axes[1].barh(single.antecedent.str.replace('MODALIDAD=', '').str[:55], single.lift, color=NAVY)
    axes[1].axvline(1, ls='--', c=GREY), axes[1].set_title('Lift de un solo postor (modalidades competitivas)')
    axes[1].tick_params(axis='y', labelsize=8)
    save(fig, '12_reglas')


def signals():
    summary = csv('integrity/summary.csv')
    summary = summary[summary.year.astype(str) == 'Todos']
    co = csv('integrity/cooccurrence.csv').dropna(subset=['lift']).sort_values('lift')
    counts = csv('integrity/count_distribution.csv')
    fig, axes = plt.subplots(1, 3, figsize=(16, 3.7))
    axes[0].barh(summary.label, summary.rate * 100, color=[ORANGE, NAVY, BROWN, TEAL][: len(summary)])
    for i, (rate, flagged) in enumerate(zip(summary.rate, summary.flagged)):
        axes[0].text(rate * 100, i, f' {rate:.1%} ({miles(flagged)})', va='center', fontsize=8)
    axes[0].set_title('Señales de riesgo (% de procesos evaluables)'), axes[0].set_xlim(0, summary.rate.max() * 160)
    axes[1].barh(co.label_a.str[:20] + ' + ' + co.label_b.str[:20], co.lift, color=TEAL)
    axes[1].axvline(1, ls='--', c=GREY), axes[1].set_title('Coincidencia de señales (lift)')
    axes[1].tick_params(axis='y', labelsize=8)
    axes[2].bar(counts.signal_count.astype(str), counts.processes, color=NAVY)
    (
        axes[2].set_yscale('log'),
        axes[2].set_xlabel('señales por proceso'),
        axes[2].set_title('Procesos por nº de señales'),
    )
    save(fig, '13_senales')


def timings():
    run = doc('metadata/run_manifest.json')
    stages = pd.DataFrame(run['stages'])
    names = {
        'validation': 'Validación',
        'processing': 'Procesamiento (Spark)',
        'part1': 'Parte I',
        'part2': 'Parte II',
        'part3': 'Parte III',
        'part4': 'Parte IV',
        'part5': 'Parte V',
        'signals': 'Señales (Q4)',
        'export': 'Exportación',
    }
    fig, ax = plt.subplots(figsize=(9, 3.4))
    ax.barh(stages.name.map(names).fillna(stages.name)[::-1], stages.seconds[::-1] / 60, color=TEAL)
    (
        ax.set_xlabel('minutos'),
        ax.set_title(f"Duración de la corrida ({run['profile']}, {stages.seconds.sum() / 60:.0f} min)"),
    )
    save(fig, '14_tiempos')


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    print('Figuras en', OUT)
    for figure in [
        kdd_funnel,
        monthly_and_amounts,
        items,
        participation,
        spark,
        concentration,
        competition,
        similarity,
        lsh_groups,
        ann,
        streams,
        rules,
        signals,
        timings,
    ]:
        figure()


if __name__ == '__main__':
    main()
