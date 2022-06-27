#!/bin/bash

declare -r KEY_SPACE='hw8_kukhar'

declare -r ddl="
CREATE KEYSPACE ${KEY_SPACE} WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE ${KEY_SPACE};

    
CREATE TABLE transactions(
    step int,
    type text,
    amount float, 
    nameOrig text, 
    oldbalanceOrg float, 
    newbalanceOrig float,
    nameDest text, 
    oldbalanceDest float, 
    newbalanceDest float, 
    isFraud int,
    isFlaggedFraud int,
    transaction_date date,
    PRIMARY KEY (nameOrig, amount))
    WITH CLUSTERING ORDER BY (amount DESC);
   

"

echo "Started creating tables"
RET=1
while [ ${RET} -eq 1 ]; do
    docker exec -it cassandra-node cqlsh -e "${ddl}"
    RET=$?
    sleep 5
done;	
echo "Finished creating tables"
