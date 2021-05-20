Prequisites
----------

Kafka
Spark
MySQL

Steps for creating the environment
----------------------------------

sudo docker pull mysql


docker run -p 3306:3306 --name mysql-test -e MYSQL_ROOT_PASSWORD=root -d mysql


docker exec -it mysql-test /bin/bash


mysql -u root -p 
root


create table tweet_texts_new5(tweet_id bigint(50) unsigned NOT NULL, window_start timestamp NOT NULL, window_end timestamp NOT NULL, count INT NOT NULL, PRIMARY KEY (tweet_id, window_start, window_end));


docker network create --subnet=172.20.0.0/16 kafkanet # custom network


1. Create zookeeper container


docker run -d --hostname zookeepernode --net kafkanet --ip 172.20.1.3 --name kafka-zookeeper --publish 2181:2181 zookeeper:3.4


2. Create Kafka container


docker run -d --hostname kafkarnode --net kafkanet --ip 172.20.1.4 --name data-kafka --publish 9092:9092 --publish 7203:7203 ches/kafka


docker exec -it <containerid> bash


./zookeeper-server-start.sh /usr/local/Cellar/kafka/2.1.0/libexec/config/zookeeper.properties


./kafka-server-start.sh /usr/local/Cellar/kafka/2.1.0/libexec/config/server.properties


./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-new-topic


./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-new-topic --from-beginning



Run the python sycrip which calls the Twitter API and push the payoad json to Kafka topic


python3 kafka-twitter.py


Clone the repo and run the StreamingSampleApp.scala file to see the data where I am counting the number of tweets produced per user_id in a timinng window.



Finally, writing all the results to the MySQL



Spark Structured Streaming maintains the checkpointing itself if you give a filesystem path while writing the output, this will help the Spark job to restart efficiently after the job failure. Checkpointing allows exactly-once semantics and also fault tolerance.



Modify Line no.77 to the local filesysytem path





