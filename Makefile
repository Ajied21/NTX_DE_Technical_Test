include .env

help:
	@echo "## docker-build			- Build Docker Images (amd64) including its inter-container network."
	@echo "## spark					- Run a Spark cluster, rebuild the postgres container, then create the destination tables "
	@echo "## airflow				- Build to orchestrator"
	@echo "## postgres				- Run database of relationship"
	@echo "## grafana				- Monitoring real-time data"
	@echo "## clean					- Cleanup all running containers related to the challenge."

docker-build:
	@echo '__________________________________________________________'
	@echo 'Building Docker Images ...'
	@echo '__________________________________________________________'
	@docker network inspect NTX-Network >/nul 2>&1 || docker network create NTX-Network
	@echo '__________________________________________________________'
	@docker build -t NTX/airflow -f ./docker/Dockerfile.airflow .
	@echo '__________________________________________________________'
	@docker build -t NTX/spark -f ./docker/Dockerfile.spark .
	@echo '==========================================================='

spark:
	@echo '__________________________________________________________'
	@echo 'Creating Spark Instance ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-spark.yaml --env-file .env up -d
	@echo '==========================================================='

spark-submit-test-product:
	@docker exec ${SPARK_WORKER_CONTAINER_NAME} \
		spark-submit \
		--master spark://${SPARK_MASTER_HOST_NAME}:${SPARK_MASTER_PORT} \
		/spark-scripts/data_ingestion_products.py

spark-submit-test-transaction:
	@docker exec ${SPARK_WORKER_CONTAINER_NAME} \
		spark-submit \
		--master spark://${SPARK_MASTER_HOST_NAME}:${SPARK_MASTER_PORT} \
		/spark-scripts/data_ingestion_transactions.py

spark-submit-test-extract:
	@docker exec ${SPARK_WORKER_CONTAINER_NAME} \
		spark-submit \
		--master spark://${SPARK_MASTER_HOST_NAME}:${SPARK_MASTER_PORT} \
		/spark-scripts/resources/extract.py

postgresql: postgres postgres-create-database-data-lake postgres-create-database-data-transform

postgres:
	@docker compose -f ./docker/docker-compose-postgres.yaml --env-file .env up -d
	@echo '__________________________________________________________'
	@echo 'Postgres container created at port ${POSTGRES_PORT}...'
	@echo '__________________________________________________________'
	@echo 'Postgres Docker Host	: ${POSTGRES_CONTAINER_NAME}' &&\
		echo 'Postgres Account	: ${POSTGRES_USER}' &&\
		echo 'Postgres password	: ${POSTGRES_PASSWORD}'
	@echo '==========================================================='

postgres-create-database-data-lake:
	@echo '__________________________________________________________'
	@echo 'Creating DB data lake...'
	@echo '_________________________________________'
	@docker exec -it ${POSTGRES_CONTAINER_NAME} psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -f ./database/data_lake.sql
	@echo '==========================================================='

postgres-create-database-data-transform:
	@echo '__________________________________________________________'
	@echo 'Creating DB data transform...'
	@echo '_________________________________________'
	@docker exec -it ${POSTGRES_CONTAINER_NAME} psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -f ./database/data_transform.sql
	@echo '==========================================================='

airflow:
	@echo '__________________________________________________________'
	@echo 'Creating Airflow Instance ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-airflow.yaml --env-file .env up -d
	@echo '==========================================================='

grafana:
	@echo '__________________________________________________________'
	@echo 'Creating Grafana Instance ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-grafana.yaml up -d
	@echo '==========================================================='

clean:
	@bash ./scripts/goodnight.sh