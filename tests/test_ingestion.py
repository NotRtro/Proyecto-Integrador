import pytest

from src.ingestion import validate_files


def test_input_files_rejects_same_year_compressed_and_plain(tmp_path, monkeypatch):
    monkeypatch.setattr(validate_files, 'RAW', tmp_path)
    (tmp_path / 'peru_oece_bulk_2024.jsonl').write_text('{}')
    (tmp_path / 'peru_oece_bulk_2024.jsonl.gz').write_bytes(b'')
    with pytest.raises(ValueError, match='comprimido y sin comprimir'):
        validate_files.input_files()


def test_input_files_ignores_partial_downloads(tmp_path, monkeypatch):
    monkeypatch.setattr(validate_files, 'RAW', tmp_path)
    (tmp_path / 'peru_oece_bulk_2023.jsonl.gz').write_bytes(b'')
    (tmp_path / 'peru_oece_bulk_2024.jsonl.gz.part').write_bytes(b'')
    assert [p.name for p in validate_files.input_files()] == ['peru_oece_bulk_2023.jsonl.gz']


def test_provenance_uses_download_manifest(tmp_path):
    path = tmp_path / 'peru_oece_bulk_2023.jsonl.gz'
    downloads = {
        '2023': dict(
            file=path.name,
            url='https://example.org/2023',
            license='CC BY 4.0',
            last_modified='Sun, 04 Oct 2026 14:00:25 GMT',
            downloaded_at='2026-10-10',
        )
    }
    assert validate_files.provenance(path, downloads)['source_url'] == 'https://example.org/2023'
    assert validate_files.provenance(tmp_path / 'otro.jsonl', downloads)['downloaded_at'] is None


def test_validation_reuses_manifest_when_files_unchanged(tmp_path, monkeypatch):
    from src import common

    raw, artifacts = tmp_path / 'raw', tmp_path / 'artifacts'
    raw.mkdir()
    (raw / 'peru_oece_bulk_2024.jsonl').write_text('{"ocid": "a", "tender": {"datePublished": "2024-01-01"}}\n')
    for module in (validate_files, common):
        monkeypatch.setattr(module, 'ARTIFACTS', artifacts)
    monkeypatch.setattr(validate_files, 'RAW', raw)
    first = validate_files.run()
    calls = []
    monkeypatch.setattr(validate_files, 'scan', lambda *a: calls.append(a))
    assert validate_files.run() == first and not calls  # sin cambios: no se vuelve a leer
    # Si el archivo cambia (tamaño distinto), se valida de nuevo.
    (raw / 'peru_oece_bulk_2024.jsonl').write_text('{"ocid": "b", "tender": {"datePublished": "2024-01-01"}}\n\n')
    assert validate_files.cached_manifest(validate_files.input_files()) is None
