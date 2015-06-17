# Makefile for NestedLoopJoin both single machine and multi-nodes  
HADOOP_VERSION=0.20.2
HADOOP_HOME=/home/hadoop/cz/hadoop-${HADOOP_VERSION}
all: 
	rm -rf knn
	mkdir knn
	javac -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar -g -d knn *.java
	jar -cvf ./knn.jar -C knn/ .     # for hadoop
clean:
	rm -rf knn
	rm knn.jar
