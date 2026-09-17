import json
import math
from datetime import datetime
from src.processing.normalize_text import normalize_text, shingles


def effective_date(record):
    tender = record.get('tender') or {}
    for source, value in [('tender.datePublished', tender.get('datePublished')), ('tender.tenderPeriod.startDate', (tender.get('tenderPeriod') or {}).get('startDate'))]:
        if value:
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00')).date().isoformat(), source
            except (ValueError, TypeError):
                pass
    return None, 'missing'


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
    text = ' '.join(str(x) for x in [t.get('title') or '', t.get('description') or ''] + [i.get('description') or '' for i in items] + [(i.get('classification') or {}).get('description') or '' for i in items])
    amount = amount_pen(t.get('value'))
    amounts = [amount_pen(a.get('value')) for a in awards]
    valid = [a for a in amounts if a is not None and a >= 0]
    bidders = number(t.get('numberOfTenderers'))
    competition = 'sin dato' if bidders is None or bidders < 1 else 'uno' if bidders == 1 else 'dos a tres' if bidders <= 3 else 'cuatro o mas'
    base = dict(ocid=record.get('ocid'), source_file=source, release_date=record.get('date') or '', effective_date=date, date_source=date_source, year=int(date[:4]) if date else None, month=int(date[5:7]) if date else None)
    p = dict(base, buyer_id=buyer.get('id'), buyer_name=buyer.get('name'), department=department, category=t.get('mainProcurementCategory'), method=t.get('procurementMethod'), method_details=t.get('procurementMethodDetails'), tender_amount_pen=amount if amount is None or amount >= 0 else None, tender_amount_original=number((t.get('value') or {}).get('amount')), currency=(t.get('value') or {}).get('currency'), negative_amount=amount is not None and amount < 0, award_amount_pen=sum(valid) if valid else None, award_unconverted_count=sum(a is None for a in amounts), bidders=bidders, competition=competition, amount_band=amount_band(amount), text=text, normalized_text=normalize_text(text), shingles=shingles(text), item_count=len(items), award_count=len(awards), supplier_count=len({s.get('id') for a in awards for s in a.get('suppliers', []) if s.get('id')}))
    tables = {'processes': [p], 'items': [], 'awards': [], 'suppliers': [], 'contracts': []}
    for idx, i in enumerate(items):
        tables['items'].append(dict(base, item_id=str(i.get('id', idx)), description=i.get('description'), classification=(i.get('classification') or {}).get('id'), quantity=number(i.get('quantity')), amount_pen=amount_pen(i.get('totalValue'))))
    for idx, a in enumerate(awards):
        aid = str(a.get('id', idx))
        suppliers = {s.get('id') or s.get('name'): s for s in a.get('suppliers', [])}
        av = amount_pen(a.get('value'))
        av = av if av is None or av >= 0 else None
        tables['awards'].append(dict(base, award_id=aid, amount_pen=av, currency=(a.get('value') or {}).get('currency'), supplier_count=len(suppliers)))
        for sid, s in suppliers.items():
            tables['suppliers'].append(dict(base, award_id=aid, supplier_id=sid, supplier_name=s.get('name'), buyer_id=buyer.get('id'), allocated_amount_pen=av / len(suppliers) if av is not None else None))
    for idx, c in enumerate(record.get('contracts') or []):
        tables['contracts'].append(dict(base, contract_id=str(c.get('id', idx)), award_id=c.get('awardID'), amount_pen=amount_pen(c.get('value')), status=c.get('status')))
    return tables
