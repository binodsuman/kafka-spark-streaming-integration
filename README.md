# kafka-spark-streaming-integration


This code base are the part of YouTube Binod Suman Academy Channel for End to end data pipeline implementation from 
scratch with Kafka Spark Streaming Integration.

Kafka Spark Streaming Integration in java from scratch | Code walk through. https://youtu.be/UcWi3-FODjs

There are two Maven Project:
1. kafka  
2. Spark  

One input file is there tet.csv   

Before starting executing these code, you have should have already:
1. Kafka server running  
2. Kafka Topic (demo) shoud be created.  
3. kafka producer and consumer console started for testing purpose.

These YouTube Video will help you to undertand better Kafka, Spark and setup.  

Kakfa Installation 
https://youtu.be/D7IY_CwXUKc

Kafka Introduction:
https://youtu.be/HtrKp6V9KuE

Kafka python
https://youtu.be/HIz0pUXhM3U

Kafka Java
https://youtu.be/5AENxG_Bvns


Spark installation on Windows | without Virtual box | Hands on
https://youtu.be/7QAveURI8ZI

Spark Introduction
https://youtu.be/cfiXvwnCz3E


----- Some Useful Kafka Commands --------<br/>
Start Zookeeper<br/>
zookeeper-server-start.sh config\zookeeper.properties<br/>
<br/>
Start Kafka Broker<br/>
kafka-server-start.sh config/server.properties<br/>
<br/>
Create topic<br/>
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test<br/>
<br/>
List topic<br/>
kafka-topics.sh --list --zookeeper localhost:2181<br/>
<br/>
Start Producer<br/>
kafka-console-producer.sh --broker-list localhost:9092 --topic test<br/>
<br/>
Send message<br/>
How are you<br/>
Binod Suman Academy<br/>
<br/>
Receive message<br/>
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning<br/>
How are you<br/>
Binod Suman Academy<br/>


