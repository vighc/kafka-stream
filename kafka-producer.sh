#Install Java
sudo apt update && sudo apt upgrade -y
sudo apt-get install openjdk-17-jdk
java -version

#Download Kafka 3.5.1
wget https://downloads.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz
tar -xzf kafka_2.13-3.5.1.tgz
cd kafka_2.13-3.5.1

#Start zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties
#nohup bin/zookeeper-server-start.sh config/zookeeper.properties >> zookeeper.txt &

#open another window
bin/kafka-server-start.sh config/server.properties
#nohup bin/kafka-server-start.sh config/server.properties >> server.txt &

#create topic
bin/kafka-topics.sh --create --topic kafka-topic --bootstrap-server <local host>:9092

#describe topic
bin/kafka-topics.sh --describe --topic kafka-topic --bootstrap-server <local host>:9092

# build Consumer window
bin/kafka-console-consumer.sh --topic kafka-topic --bootstrap-server <local host>:9092

# produce kafka massage
bin/kafka-console-producer.sh --topic kafka-topic --bootstrap-server <local host>:9092


#Stop zookeeper
bin/zookeeper-server-stop.sh

#stop server
bin/kafka-server-stop.sh