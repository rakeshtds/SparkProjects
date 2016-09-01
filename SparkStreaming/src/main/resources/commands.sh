cd $AUREA_SCRIPT_HOME
#start zookeeper
./zookeeper.sh start
#start kafka
./kafka.sh start

#go to kafka home and add data into topic
cd $KAFKA_HOME
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test < /home/hadoop/interests/streamingapp/streamingapp/core/src/main/resources/source/one.csv
