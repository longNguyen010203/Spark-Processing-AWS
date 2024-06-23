init:
	docker-compose up airflow-init

up:
	docker-compose up -d

terra_init:
	terraform init

validate:
	terraform validate

plan:
	terraform plan

apply:
	terraform apply

re-init:
	terraform init --configure