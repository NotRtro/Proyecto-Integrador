import json
import math
from datetime import datetime
from src.processing.entity_level import buyer_level
from src.processing.normalize_text import normalize_text


def effective_date(record):
    tender = record.get('tender') or {}
    for source, value in [('tender.datePublished', tender.get('datePublished')), ('tender.tenderPeriod.startDate', (tender.get('tenderPeriod') or {}).get('startDate'))]:
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
    value = value or {}
    converted = number(value.get('amount_PEN'))
    return converted if converted is not None else number(value.get('amount')) if value.get('currency') == 'PEN' else None


def converted_amount(value, implied_currency=None, implied_rate=None):
    """Return PEN value plus auditable conversion metadata.

    The source-provided amount_PEN always wins. Foreign awards may reuse the
    tender's own PEN/original ratio when both objects use the same currency.
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
    if value is None or value < 0:
        return 'sin dato'
    return 'bajo' if value < 100000 else 'medio' if value < 1000000 else 'alto' if value < 10000000 else 'muy alto'


def flatten(record, source):
    t = record.get('tender') or {}
    date, date_source = effective_date(record)
    buyer = record.get('buyer') or t.get('procuringEntity') or {}
    address = next(((p.get('address') or {}) for p in record.get('parties', []) if p.get('id') == buyer.get('id')), {})
    department = normalize_text(address.get('department') or address.get('region')).upper() or None
    items = t.get('items') or []
    awards = record.get('awards') or []
    # El título OCDS del OECE es la nomenclatura del proceso (AS-SM-12-2024-MDCH/CS-1), no su
    # objeto: se guarda como tender_code y se excluye del texto usado para similitud.
    text = ' '.join(str(x) for x in [t.get('description') or '']
                    + [i.get('description') or '' for i in items]
                    + [(i.get('classification') or {}).get('description') or '' for i in items])
    tender_value = t.get('value') or {}
    amount, tender_conversion_method, tender_exchange_rate = converted_amount(tender_value)
    tender_original = number(tender_value.get('amount'))
    tender_currency = tender_value.get('currency')
    implied_rate = tender_exchange_rate if amount is not None and tender_original not in (None, 0) else None
    converted_awards = [converted_amount(a.get('value'), tender_currency, implied_rate) for a in awards]
    amounts = [converted[0] for converted in converted_awards]
    valid = [a for a in amounts if a is not None and a >= 0]
    bidders = number(t.get('numberOfTenderers'))
    competition = 'sin dato' if bidders is None or bidders < 1 else 'uno' if bidders == 1 else 'dos a tres' if bidders <= 3 else 'cuatro o mas'
    base = dict(ocid=record.get('ocid'), source_file=source, release_date=record.get('date') or '', effective_date=date, date_source=date_source, year=int(date[:4]) if date else None, month=int(date[5:7]) if date else None)
    p = dict(
        base,
        buyer_id=buyer.get('id'), buyer_name=buyer.get('name'), buyer_level=buyer_level(buyer.get('name')),
        department=department, category=t.get('mainProcurementCategory'), method=t.get('procurementMethod'),
        method_details=t.get('procurementMethodDetails'), tender_code=t.get('title'),
        tender_amount_pen=amount if amount is None or amount >= 0 else None, tender_amount_original=tender_original,
        currency=tender_currency, tender_conversion_method=tender_conversion_method,
        tender_exchange_rate=tender_exchange_rate, negative_amount=amount is not None and amount < 0,
        extreme_amount=amount is not None and amount > 1000000000,
        award_amount_pen=sum(valid) if valid else None,
        award_converted_count=sum(method == 'tender_implied_rate' for _, method, _ in converted_awards),
        award_unconverted_count=sum(a is None for a in amounts),
        bidders=bidders, competition=competition, amount_band=amount_band(amount),
        text=text, normalized_text=normalize_text(text),
        item_count=len(items), award_count=len(awards),
        supplier_count=len({s.get('id') for a in awards for s in a.get('suppliers', []) if s.get('id')}),
    )
    tables = {'processes': [p], 'items': [], 'awards': [], 'suppliers': [], 'contracts': []}
    for idx, i in enumerate(items):
        item_value = i.get('totalValue') or {}
        item_amount, item_method, item_rate = converted_amount(item_value)
        tables['items'].append(dict(base, item_id=str(i.get('id', idx)), description=i.get('description'), classification=(i.get('classification') or {}).get('id'), quantity=number(i.get('quantity')), amount_original=number(item_value.get('amount')), amount_pen=item_amount, currency=item_value.get('currency'), conversion_method=item_method, exchange_rate=item_rate, conversion_reference_date=date))
    for idx, (a, converted) in enumerate(zip(awards, converted_awards)):
        aid = str(a.get('id', idx))
        suppliers = {s.get('id') or s.get('name'): s for s in a.get('suppliers', [])}
        award_value = a.get('value') or {}
        av, conversion_method, exchange_rate = converted
        av = av if av is None or av >= 0 else None
        award_date = iso_date(a.get('date')) or date
        tables['awards'].append(dict(base, award_id=aid, amount_original=number(award_value.get('amount')), amount_pen=av, currency=award_value.get('currency'), conversion_method=conversion_method, exchange_rate=exchange_rate, conversion_reference_date=award_date, supplier_count=len(suppliers)))
        for sid, s in suppliers.items():
            tables['suppliers'].append(dict(base, award_id=aid, supplier_id=sid, supplier_name=s.get('name'), buyer_id=buyer.get('id'), allocated_amount_pen=av / len(suppliers) if av is not None else None, currency=award_value.get('currency'), conversion_method=conversion_method, exchange_rate=exchange_rate, conversion_reference_date=award_date))
    for idx, c in enumerate(record.get('contracts') or []):
        contract_value = c.get('value') or {}
        contract_amount, contract_method, contract_rate = converted_amount(contract_value)
        reference_date = iso_date(c.get('dateSigned')) or iso_date((c.get('period') or {}).get('startDate')) or date
        tables['contracts'].append(dict(base, contract_id=str(c.get('id', idx)), award_id=c.get('awardID'), amount_original=number(contract_value.get('amount')), amount_pen=contract_amount, currency=contract_value.get('currency'), conversion_method=contract_method, exchange_rate=contract_rate, conversion_reference_date=reference_date, status=c.get('status')))
    return tables
