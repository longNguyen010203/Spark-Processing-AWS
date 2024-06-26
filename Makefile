init:
	docker-compose up airflow-init

up:
	docker-compose up -d

down:
	docker-compose down

build:
	docker-compose build

restart:
	make down && make up

down-v:
	docker-compose down -v