.PHONY: all build up download run rapido test down dev-sample dev static notebook
all: build up download run
build:
	CODE_VERSION=$$(git rev-parse --short HEAD) docker compose build
up:
	docker compose up -d --wait spark-master spark-worker-1 spark-worker-2 api web
download:
	docker compose run --rm --no-deps pipeline python -m src.ingestion.download
run:
	docker compose run --rm pipeline python -m src.run_all
# Corrida de trabajo: ANN sobre 50 mil procesos y mediciones reutilizadas (la web, Datos y KDD → Decisiones de volumen).
rapido:
	docker compose run --rm pipeline python -m src.run_all --profile rapido
test:
	docker compose run --rm --no-deps -e SPARK_MASTER_URL='local[2]' -e SPARK_DRIVER_HOST= pipeline python -m pytest -q
	docker compose up -d --wait api web
	docker compose run --rm --no-deps -v "$(CURDIR)/scripts:/checks:ro" api python /checks/test_api.py
	docker compose run --rm --no-deps -v "$(CURDIR)/scripts:/checks:ro" api python /checks/test_frontend.py
down:
	docker compose down
# Modo muestra: ~3 % de las líneas para probar el pipeline en minutos (cifras no válidas para el informe).
dev-sample:
	docker compose run --rm --no-deps pipeline python scripts/make_dev_sample.py
dev: export DATA_DIR = ./data/dev
dev: export ARTIFACTS_DIR = ./data/dev/artifacts
dev:
	docker compose up -d --wait spark-master spark-worker-1 spark-worker-2 api web
	docker compose run --rm pipeline python -m src.run_all
# Web estática para publicar (GitHub Pages): exporta las respuestas de la API a frontend/public/data.
static:
	docker compose run --rm --no-deps -v "$(CURDIR)/frontend/public:/out" api python -m api.export_static /out/data
# Cuaderno de sustento: genera notebooks/sustento.ipynb y lo ejecuta sobre los artefactos (gráficos + comprobaciones).
notebook:
	docker compose run --rm --no-deps -v "$(CURDIR):/workspace" pipeline sh -c "pip install -q -r requirements-notebook.txt && python scripts/build_notebook.py && jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=600 notebooks/sustento.ipynb"
