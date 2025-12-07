
### Building the environment
docker compose --project-name lp --project-directory . --env-file ./env/all.env up -d --build

### You MUST down the service to run it seprately
docker compose --project-name lp --project-directory . --env-file ./env/all.env down

### Opening kafka environment 
docker compose --project-name lp --project-directory . --env-file ./env/all.env up -d postgres redis broker schema-registry kafka-ui pgadmin

### Starting kafka Producer
docker compose --project-name lp --project-directory . --env-file ./env/all.env up -d producer

### Starting Airflow services
docker compose --project-name lp --project-directory . --env-file ./env/all.env up -d airflow-init

### Starting The remaining services
docker compose --project-name lp --project-directory . --env-file ./env/all.env up -d 





### Starting only the databases and kafka services
docker compose --project-name lp --env-file ./env/all.env up -d postgres redis broker kafka-ui pgadmin

### Starting only the producer service
docker compose --project-name lp --project-directory . --env-file ./env/all.env start producer

### Setting the last order number in Redis 
docker compose --project-name lp --project-directory . --env-file ./env/all.env exec redis redis-cli SET sales_last_order_number 14999

### Restarting Redis service
docker compose --project-name lp --project-directory . --env-file ./env/all.env restart redis

### Starting the Spark Notebook service and cassandra database
docker compose --project-name lp --project-directory . --env-file ./env/all.env start spark-notebook cassandra

### Starting the sink-service and grafana services
docker compose --project-name lp --project-directory . --env-file ./env/all.env start sink-service grafana

docker compose --project-name lp --project-directory . --env-file ./env/all.env up -d cassandra 


docker compose --project-name lp --project-directory . --env-file ./env/all.env up -d sink-service grafana 


###Downloading S3 Sink Connector JAR
# mkdir -p ./connectors
# docker run --rm -v ${PWD}/connectors:/tmp/connectors confluentinc/cp-kafka-connect:7.9.1 bash -c "confluent-hub install --no-prompt confluentinc/kafka-connect-s3:latest --component-dir /tmp/connectors"


# Create S3 Sink Connector
curl.exe -X POST -H "Content-Type: application/json" --data @s3-sink-config.json http://localhost:8083/connectors

# Delete Connector
curl.exe -X DELETE http://localhost:8083/connectors/s3-sales-events-sink

# Check Connector Status
Invoke-WebRequest -Uri "http://localhost:8083/connectors/s3-sales-events-sink/status"  