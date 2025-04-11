PROJECT_NAME = aml-detection

DOCKER_COMPOSE_SQLSERVER = docker-compose -f ./docker-compose.base.yml -f ./docker-compose.sqlserver.yml

DOCKER_COMPOSE_HADOOP = docker-compose -f ./docker-compose.base.yml -f ./docker-compose.hadoop.yml
DOCKER_COMPOSE_SPARK = docker-compose -f ./docker-compose.base.yml -f ./docker-compose.spark.yml

DOCKER_COMPOSE_AIRFLOW = docker-compose -f ./docker-compose.base.yml -f ./docker-compose.airflow.yml

DOCKER_COMPOSE_NEO4J = docker-compose -f ./docker-compose.base.yml -f ./docker-compose.neo4j.yml

DOCKER_COMPOSE_KAFKA = docker-compose -f ./docker-compose.base.yml -f ./docker-compose.kafka.yml

DOCKER_COMPOSE_FLINK = docker-compose -f ./docker-compose.base.yml -f ./docker-compose.flink.yml

DOCKER_COMPOSE_NOTEBOOK = docker-compose -f ./docker-compose.base.yml -f ./docker-compose.notebook.yml


up-sqlserver:
	$(DOCKER_COMPOSE_SQLSERVER) up
down-sqlserver:
	$(DOCKER_COMPOSE_SQLSERVER) down


up-hadoop:
	$(DOCKER_COMPOSE_HADOOP) up
down-hadoop:
	$(DOCKER_COMPOSE_HADOOP) down


up-spark:
	$(DOCKER_COMPOSE_SPARK) up
down-spark:
	$(DOCKER_COMPOSE_SPARK) down
build-spark:
	$(DOCKER_COMPOSE_SPARK) build


up-airflow:
	$(DOCKER_COMPOSE_AIRFLOW) up
down-airflow:
	$(DOCKER_COMPOSE_AIRFLOW) down
build-airflow:
	$(DOCKER_COMPOSE_AIRFLOW) build
stop-airflow:
	$(DOCKER_COMPOSE_AIRFLOW) stop


up-neo4j:
	$(DOCKER_COMPOSE_NEO4J) up
down-neo4j:
	$(DOCKER_COMPOSE_NEO4J) down
stop-neo4j:
	$(DOCKER_COMPOSE_NEO4J) stop


up-kafka:
	$(DOCKER_COMPOSE_KAFKA) up
down-kafka:
	$(DOCKER_COMPOSE_KAFKA) down
stop-kafka:
	$(DOCKER_COMPOSE_KAFKA) stop


up-flink:
	$(DOCKER_COMPOSE_FLINK) up
down-flink:
	$(DOCKER_COMPOSE_FLINK) down
stop-flink:
	$(DOCKER_COMPOSE_FLINK) stop
build-flink:
	$(DOCKER_COMPOSE_FLINK) build


up-notebook:
	$(DOCKER_COMPOSE_NOTEBOOK) up
down-notebook:
	$(DOCKER_COMPOSE_NOTEBOOK) down
stop-notebook:
	$(DOCKER_COMPOSE_NOTEBOOK) stop
build-notebook:
	$(DOCKER_COMPOSE_NOTEBOOK) build