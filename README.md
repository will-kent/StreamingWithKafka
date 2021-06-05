# Streaming With Kafka and Spark
## Subject

## Aim

## Process
### Producers
There are two producers:
* Submissions
* Comments

These are started by running __python src/producer.py__ in the jupyter docker container.

### KSQLDB Objects
Once the producers have been started and the topics have been created in the broker the ksqldb objects can be created. All ksqldb objects can be found in the folder **ksqldb/base_queries**. To combine all base queries into a sinle statement that can be run on the ksqldb modify the __create_ksqldb_objects.sh__ file, as required, and the process it using bash create_ksqldb_objects.sql. A summarised file will be created in the **ksqldb** folder that can be run on the ksqldb. Be careful to ensure SQL files are in the right order to ensure no dependency issues when running in the ksqldb.

## Outcome
