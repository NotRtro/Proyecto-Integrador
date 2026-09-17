.PHONY: all build up run test down
all: build up run
build:
	docker compose build
up:
	docker compose up -d --wait spark-master spark-worker-1 spark-worker-2 web
run:
	docker compose run --rm pipeline python -m src.run_all
test:
	docker compose run --rm --no-deps -e SPARK_MASTER_URL='local[2]' -e SPARK_DRIVER_HOST= pipeline python -m pytest -q
down:
	docker compose down
