"""Aplanado de un documento OCDS en filas de las tablas analíticas.

Motor: funciones Python puras. Spark las ejecuta en paralelo dentro de
`build_processes.parse` (una llamada por documento), así que aquí no hay Spark.

Un registro OCDS anida convocatoria, ítems, adjudicaciones con proveedores y
contratos. `flatten` produce una fila de `processes` y las filas hijas de `items`,
`awards`, `suppliers` y `contracts`, todas con las columnas BASE del proceso.

Reglas de negocio documentadas en el README:
- Fecha de convocatoria: tender.datePublished; si falta, tender.tenderPeriod.startDate.
- Montos: amount_PEN de la fuente; PEN nativo; o tasa implícita de la convocatoria si
  la adjudicación está en la misma moneda. Lo demás queda nulo (nunca cero) y se
  completa después en Spark con tasas históricas.
- El monto de una adjudicación con varios proveedores se reparte en partes iguales.
"""
import math
from datetime import datetime

from src.processing.entity_level import buyer_level
from src.processing.normalize_text import normalize_text

EXTREME_AMOUNT = 1_000_000_000


def effective_date(record):
    """Fecha de convocatoria (ISO) y el campo del que se tomó."""
    tender = record.get('tender') or {}
    candidates = [('tender.datePublished', tender.get('datePublished')),
                  ('tender.tenderPeriod.startDate', (tender.get('tenderPeriod') or {}).get('startDate'))]
    for source, value in candidates:
        if value:
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00')).date().isoformat(), source
            except (ValueError, TypeError):
                pass
    return None, 'missing'


def iso_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).date().isoformat()
    except (ValueError, TypeError):
        return None


def number(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def amount_pen(value):
    """Monto en soles si la fuente lo publica (amount_PEN) o si ya está en PEN."""
    value = value or {}
    converted = number(value.get('amount_PEN'))
    if converted is not None:
        return converted
    return number(value.get('amount')) if value.get('currency') == 'PEN' else None


def converted_amount(value, implied_currency=None, implied_rate=None):
    """Monto en PEN con el método y la tasa usados, para que la conversión sea auditable.

    El amount_PEN de la fuente siempre gana. Una adjudicación en moneda extranjera
    puede reutilizar la tasa PEN/original de su propia convocatoria si ambas usan la
    misma moneda.
    """
    value = value or {}
    original = number(value.get('amount'))
    currency = value.get('currency')
    source_pen = number(value.get('amount_PEN'))
    if source_pen is not None:
        rate = source_pen / original if original not in (None, 0) else None
        return source_pen, 'source_amount_pen', rate
    if currency == 'PEN' and original is not None:
        return original, 'native_pen', 1.0
    if original is not None and currency and currency == implied_currency and implied_rate is not None:
        return original * implied_rate, 'tender_implied_rate', implied_rate
    return None, 'unavailable', None


def amount_band(value):
    """Rangos de monto convocado en PEN: bajo < 100 mil <= medio < 1 M <= alto < 10 M <= muy alto."""
    if value is None or value < 0:
        return 'sin dato'
    if value < 100_000:
        return 'bajo'
    if value < 1_000_000:
        return 'medio'
    return 'alto' if value < 10_000_000 else 'muy alto'


MODALITIES = [
    # (prefijo normalizado de procurementMethodDetails, modalidad). El orden importa: los
    # nombres más largos primero ("licitacion publica abreviada" antes que "licitacion publica").
    ('adjudicacion simplificada', 'Adjudicación Simplificada'),
    ('subasta inversa', 'Subasta Inversa Electrónica'),
    ('comparacion precios', 'Comparación de Precios'),  # normalize_text quita "de"
    ('contratacion directa', 'Contratación Directa'),
    ('licitacion publica abreviada', 'Licitación Pública Abreviada'),
    ('licitacion publica', 'Licitación Pública'),
    ('concurso publico abreviado', 'Concurso Público Abreviado'),
    ('concurso publico', 'Concurso Público'),
    ('regimen especial', 'Régimen o procedimiento especial'),
    ('procedimiento especial', 'Régimen o procedimiento especial'),
    ('convenio', 'Convenio'),
    ('contratacion internacional', 'Contratación Internacional'),
    ('adjudicacion selectiva', 'Adjudicación Selectiva o Abreviada'),
    ('adjudicacion abreviada', 'Adjudicación Selectiva o Abreviada'),
]


def procurement_modality(details):
    """Modalidad peruana a partir de procurementMethodDetails.

    procurementMethod (open/selective/limited/direct) es demasiado grueso: "open" cubre
    más del 80 % de los procesos. El detalle trae la modalidad de la normativa peruana
    con variantes (homologación, decretos de urgencia); se agrupan en su modalidad base.
    """
    if not details:
        return None
    text = normalize_text(details)
    return next((name for prefix, name in MODALITIES if text.startswith(prefix)), 'Otra')


def competition_level(bidders):
    if bidders is None or bidders < 1:
        return 'sin dato'
    if bidders == 1:
        return 'uno'
    return 'dos a tres' if bidders <= 3 else 'cuatro o mas'


def requirement_text(tender, items):
    """Descripción del requerimiento y de sus ítems (incluida su clasificación UNSPSC).

    El título OCDS del OECE es la nomenclatura del proceso (AS-SM-12-2024-MDCH/CS-1), no
    su objeto: se guarda como tender_code y se excluye del texto usado para similitud.
    """
    parts = ([tender.get('description') or '']
             + [i.get('description') or '' for i in items]
             + [(i.get('classification') or {}).get('description') or '' for i in items])
    return ' '.join(str(x) for x in parts)


def buyer_department(record, buyer):
    """Departamento del domicilio del comprador (no necesariamente del lugar de ejecución)."""
    address = next(((p.get('address') or {}) for p in record.get('parties', []) if p.get('id') == buyer.get('id')), {})
    return normalize_text(address.get('department') or address.get('region')).upper() or None


def item_rows(base, items, date):
    rows = []
    for idx, item in enumerate(items):
        value = item.get('totalValue') or {}
        amount, method, rate = converted_amount(value)
        rows.append(dict(
            base, item_id=str(item.get('id', idx)), description=item.get('description'),
            classification=(item.get('classification') or {}).get('id'), quantity=number(item.get('quantity')),
            amount_original=number(value.get('amount')), amount_pen=amount, currency=value.get('currency'),
            conversion_method=method, exchange_rate=rate, conversion_reference_date=date))
    return rows


def award_and_supplier_rows(base, awards, converted_awards, buyer_id, date):
    award_rows, supplier_rows = [], []
    for idx, (award, (amount, method, rate)) in enumerate(zip(awards, converted_awards)):
        award_id = str(award.get('id', idx))
        # Un proveedor repetido dentro de la misma adjudicación se cuenta una vez.
        suppliers = {s.get('id') or s.get('name'): s for s in award.get('suppliers', [])}
        value = award.get('value') or {}
        amount = amount if amount is None or amount >= 0 else None
        award_date = iso_date(award.get('date')) or date
        award_rows.append(dict(
            base, award_id=award_id, amount_original=number(value.get('amount')), amount_pen=amount,
            currency=value.get('currency'), conversion_method=method, exchange_rate=rate,
            conversion_reference_date=award_date, supplier_count=len(suppliers)))
        for supplier_id, supplier in suppliers.items():
            supplier_rows.append(dict(
                base, award_id=award_id, supplier_id=supplier_id, supplier_name=supplier.get('name'), buyer_id=buyer_id,
                allocated_amount_pen=amount / len(suppliers) if amount is not None else None,
                currency=value.get('currency'), conversion_method=method, exchange_rate=rate,
                conversion_reference_date=award_date))
    return award_rows, supplier_rows


def contract_rows(base, contracts, date):
    rows = []
    for idx, contract in enumerate(contracts):
        value = contract.get('value') or {}
        amount, method, rate = converted_amount(value)
        reference_date = (iso_date(contract.get('dateSigned'))
                          or iso_date((contract.get('period') or {}).get('startDate')) or date)
        rows.append(dict(
            base, contract_id=str(contract.get('id', idx)), award_id=contract.get('awardID'),
            amount_original=number(value.get('amount')), amount_pen=amount, currency=value.get('currency'),
            conversion_method=method, exchange_rate=rate, conversion_reference_date=reference_date,
            status=contract.get('status')))
    return rows


def flatten(record, source):
    """Filas de las cinco tablas para un documento OCDS."""
    tender = record.get('tender') or {}
    date, date_source = effective_date(record)
    buyer = record.get('buyer') or tender.get('procuringEntity') or {}
    items = tender.get('items') or []
    awards = record.get('awards') or []
    text = requirement_text(tender, items)

    tender_value = tender.get('value') or {}
    amount, tender_conversion_method, tender_exchange_rate = converted_amount(tender_value)
    tender_original = number(tender_value.get('amount'))
    tender_currency = tender_value.get('currency')
    implied_rate = tender_exchange_rate if amount is not None and tender_original not in (None, 0) else None
    converted_awards = [converted_amount(a.get('value'), tender_currency, implied_rate) for a in awards]
    award_amounts = [converted[0] for converted in converted_awards]
    valid_awards = [a for a in award_amounts if a is not None and a >= 0]
    bidders = number(tender.get('numberOfTenderers'))

    base = dict(ocid=record.get('ocid'), source_file=source, release_date=record.get('date') or '',
                effective_date=date, date_source=date_source,
                year=int(date[:4]) if date else None, month=int(date[5:7]) if date else None)
    process = dict(
        base,
        buyer_id=buyer.get('id'), buyer_name=buyer.get('name'), buyer_level=buyer_level(buyer.get('name')),
        department=buyer_department(record, buyer), category=tender.get('mainProcurementCategory'),
        method=tender.get('procurementMethod'), method_details=tender.get('procurementMethodDetails'),
        modality=procurement_modality(tender.get('procurementMethodDetails')),
        tender_code=tender.get('title'),
        tender_amount_pen=amount if amount is None or amount >= 0 else None, tender_amount_original=tender_original,
        currency=tender_currency, tender_conversion_method=tender_conversion_method,
        tender_exchange_rate=tender_exchange_rate, negative_amount=amount is not None and amount < 0,
        extreme_amount=amount is not None and amount > EXTREME_AMOUNT,
        award_amount_pen=sum(valid_awards) if valid_awards else None,
        award_converted_count=sum(method == 'tender_implied_rate' for _, method, _ in converted_awards),
        award_unconverted_count=sum(a is None for a in award_amounts),
        bidders=bidders, competition=competition_level(bidders), amount_band=amount_band(amount),
        text=text, normalized_text=normalize_text(text),
        item_count=len(items), award_count=len(awards),
        supplier_count=len({s.get('id') for a in awards for s in a.get('suppliers', []) if s.get('id')}),
    )
    award_rows, supplier_rows = award_and_supplier_rows(base, awards, converted_awards, buyer.get('id'), date)
    return {
        'processes': [process],
        'items': item_rows(base, items, date),
        'awards': award_rows,
        'suppliers': supplier_rows,
        'contracts': contract_rows(base, record.get('contracts') or [], date),
    }
