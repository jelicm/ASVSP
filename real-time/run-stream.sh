# !/bin/bash

docker cp postgresql-42.5.1.jar spark-master:postgresql-42.5.1.jar
docker cp ./consumer/consumer1.py spark-master:consumer1.py
docker cp ./consumer/consumer2.py spark-master:consumer2.py
docker cp ./consumer/consumer3.py spark-master:consumer3.py
docker cp ./consumer/consumer4.py spark-master:consumer4.py
docker cp ./consumer/join_consumer.py spark-master:join_consumer.py

printf "RUN STREAM PROCESSING! \n"
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars postgresql-42.5.1.jar consumer1.py &
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars postgresql-42.5.1.jar consumer2.py & 
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars postgresql-42.5.1.jar consumer3.py &
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars postgresql-42.5.1.jar consumer4.py &
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars postgresql-42.5.1.jar join_consumer.py   