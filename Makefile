.PHONY: all build up download run test down
all: build up download run
build:
	docker compose build
up:
	docker compose up -d --wait spark-master spark-worker-1 spark-worker-2 api web
download:
	docker compose run --rm --no-deps pipeline python -m src.ingestion.download
run:
	docker compose run --rm pipeline python -m src.run_all
test:
	docker compose run --rm --no-deps -e SPARK_MASTER_URL='local[2]' -e SPARK_DRIVER_HOST= pipeline python -m pytest -q
	docker compose up -d --wait api web
	docker compose run --rm --no-deps -v "$(CURDIR)/scripts:/checks:ro" api python /checks/test_api.py
	docker compose run --rm --no-deps -v "$(CURDIR)/scripts:/checks:ro" api python /checks/test_frontend.py
down:
	docker compose down
