
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
docker compose --project-name lp --env-file ./env/all.env start postgres redis broker schema-registry kafka-ui pgadmin

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
