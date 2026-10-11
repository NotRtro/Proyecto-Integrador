"""Nivel de gobierno de la entidad compradora, derivado de su nombre.

Motor: función Python pura (se usa dentro del parseo distribuido de Spark).

El OCDS del OECE no publica el nivel de gobierno (en `parties` solo hay nombre,
dirección e identificadores), así que se infiere del nombre oficial con reglas
ordenadas. El orden importa: primero los patrones inequívocos y al final los
genéricos. Por ejemplo, "INPE-DIRECCION REGIONAL LIMA" es un órgano nacional
aunque diga "REGIONAL", y "INSTITUTO VIAL PROVINCIAL" es municipal aunque diga
"INSTITUTO". La cobertura y los no clasificados se reportan en quality_summary.

Niveles: nacional, regional, local, empresa_publica, otro.
"""

import re
import unicodedata

RULES = [
    # 1. Empresas del Estado (FONAFE, municipales y regionales): se separan porque compran
    #    con reglas y escalas distintas a las de las entidades de gobierno.
    (
        'empresa_publica',
        r'\bEMPRESA\b|\bS\.?\s?A\.?\s?C?\.?(\s|-|$)|SOCIEDAD ANONIMA|PETROLEOS|PERUPETRO|ENTIDAD PRESTADORA'
        r'|\bS\.?R\.?L\b|\bEPS\b|SOCIEDAD ELECTRICA|\bELECTRO(?!NIC)|SEDAPAL|BANCO DE LA NACION|CORPAC|SERVICIOS '
        r'POSTALES'
        r'|SERVICIO DE AGUA POTABLE|CORPORACION FINANCIERA',
    ),
    # 2. Gobiernos locales y sus organismos.
    (
        'local',
        r'MUNICIPALIDAD|MUNICIPAL|CONCEJO|GOBIERNO LOCAL|INSTITUTO VIAL PROVINCIAL|MANCOMUNIDAD|CENTRO POBLADO'
        r'|SERVICIO DE ADMINISTRACION TRIBUTARIA|SERVICIOS? DE PARQUES|SISTEMA METROPOLITANO|PATRONATO'
        r'|GESTION AMBIENTAL',
    ),
    # 3. Gobiernos regionales explícitos.
    ('regional', r'GOBIERNO REGIONAL|^REGION\b(?! POLICIAL)|GOB\.? ?REG'),
    # 4. Marcadores inequívocos del gobierno nacional, incluidos sus órganos desconcentrados
    #    que llevan "regional" o "zonal" en el nombre (INPE, SUNARP, SUNAT, Poder Judicial, PNP).
    (
        'nacional',
        r'^INPE|ZONA REGISTRAL|SUNARP|SUNAT|INTENDENCIA|CORTE SUPERIOR|DISTRITO FISCAL|^MTC\b|^MINAG\b'
        r'|^MINSA\b|^MINEDU\b|^MEF\b|MINISTERIO|NACIONAL|SUPERINTENDENCIA|SEGURO SOCIAL|ESSALUD|EJERCITO'
        r'|MARINA DE GUERRA|FUERZA AEREA|POLICIA|\bPNP\b|\bRPNP\b|COMANDO|CONGRESO|PODER JUDICIAL|CONTRALORIA'
        r'|DEFENSORIA|JURADO|RENIEC|VRAEM|PROCURADURIA|PRESIDENCIA|DESPACHO|PROVIAS|PRONIED|AGRO RURAL'
        r'|PERU COMPRAS|BANCO CENTRAL|SIERRA EXPORTADORA|ZONA FRANCA|ZONA ESPECIAL DE DESARROLLO'
        r'|ZONA ARQUEOLOGICA|PERU SEGURO',
    ),
    # 5. Unidades típicamente regionales (salud y educación descentralizadas).
    (
        'regional',
        r'REGIONAL|HOSPITAL|RED DE SALUD|RED ASISTENCIAL|MICRORRED|MICRO RED|\bUGEL\b'
        r'|UNIDAD DE GESTION EDUCATIVA|DIRESA|GERESA|PROYECTO ESPECIAL|UNIDAD EJECUTORA|SALUD',
    ),
    # 6. Términos genéricos de organismos del gobierno nacional, al final para no capturar
    #    "PROGRAMA REGIONAL" o "INSTITUTO VIAL PROVINCIAL".
    (
        'nacional',
        r'ORGANISMO|TRIBUNAL|COMISION|CENTRAL DE COMPRAS|AGENCIA|AUTORIDAD|FONDO|IAFAS|PROGRAMA'
        r'|SERVICIO NACIONAL|REGISTRO|ARCHIVO GENERAL|BIBLIOTECA|ACADEMIA|ESCUELA|INSTITUTO'
        r'|CENTRO DE ALTOS ESTUDIOS|OFICINA|SECRETARIA|CUERPO GENERAL|CENTRO VACACIONAL',
    ),
]
COMPILED = [(level, re.compile(pattern)) for level, pattern in RULES]
LEVELS = ['nacional', 'regional', 'local', 'empresa_publica', 'otro']


def normalize_name(name):
    text = unicodedata.normalize('NFKD', name or '').encode('ascii', 'ignore').decode().upper()
    return re.sub(r'\s+', ' ', text).strip()


def buyer_level(name):
    clean = normalize_name(name)
    for level, pattern in COMPILED:
        if pattern.search(clean):
            return level
    return 'otro'
