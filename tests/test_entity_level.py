import pytest

from src.processing.entity_level import buyer_level


@pytest.mark.parametrize(
    'name, level',
    [
        ('MUNICIPALIDAD DISTRITAL DE CHALACO', 'local'),
        ('INSTITUTO VIAL PROVINCIAL DE HUANCAYO', 'local'),
        ('CONCEJO DISTRITAL DE SAN JUAN DE SIGUAS', 'local'),
        ('GOBIERNO REGIONAL DE PIURA - UNIDAD DE GESTION EDUCATIVA LOCAL AYABACA', 'regional'),
        ('HOSPITAL EMERGENCIA ATE VITARTE', 'regional'),
        ('INPE-DIRECCION REGIONAL LIMA', 'nacional'),
        ('ZONA REGISTRAL N° VI SEDE PUCALLPA', 'nacional'),
        ('REGION POLICIAL HUANUCO - SAN MARTIN - UCAYALI', 'nacional'),
        ('UNIVERSIDAD NACIONAL DE BARRANCA', 'nacional'),
        ('MINISTERIO PUBLICO - CARPETA FISCAL ELECTRONICA', 'nacional'),
        ('SEGURO SOCIAL DE SALUD', 'nacional'),
        ('EMPRESA REGIONAL DE SERVICIO PUBLICO DE ELECTRICIDAD DEL CENTRO SA ELECTROCENTRO S.A.', 'empresa_publica'),
        ('SERVICIO DE AGUA POTABLE Y ALCANTARILLADO DE LIMA - SEDAPAL', 'empresa_publica'),
        ('PETROLEOS DEL PERU S.A.', 'empresa_publica'),
        ('', 'otro'),
        (None, 'otro'),
    ],
)
def test_buyer_level(name, level):
    assert buyer_level(name) == level
