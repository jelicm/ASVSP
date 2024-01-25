# !/bin/bash

docker cp preprocessing.py spark-master:preprocessing.py

docker exec spark-master ./spark/bin/spark-submit preprocessing.py
printf "PREPROCESSING FINISHED! \n"

docker cp query.py spark-master:query.py
docker cp postgresql-42.5.1.jar spark-master:postgresql-42.5.1.jar

docker exec spark-master ./spark/bin/spark-submit  --jars postgresql-42.5.1.jar query.py
printf " QUERIES EXECUTED! \n"