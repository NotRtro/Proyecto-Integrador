def transaction(row):
    return sorted([f'ANIO={row["year"]}', f'CAT={row["category"] or "sin dato"}', f'METODO={row["method"] or "sin dato"}', f'DEP={row["department"] or "sin dato"}', f'MONTO={row["amount_band"]}', f'POSTORES={row["competition"]}'])
