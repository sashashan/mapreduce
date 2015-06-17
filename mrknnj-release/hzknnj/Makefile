HADOOP_VERSION=0.20.2
HADOOP_HOME=/home/hadoop/cz/hadoop-${HADOOP_VERSION}

JLIBS=${HADOOP_HOME}/lib/commons-lang-2.5.jar
MLIBS=util.jar:collections.jar:disc.jar#You need to download these packages, see readme.txt
JPath=JPATH=/usr/java/jdk1.7.0_03/bin#sun java
JC=${JPath}/javac 

all:
	${JC} -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar:${JLIBS}:${MLIBS} -g  -d knn *.java
	jar -cvf ./knn.jar -C knn/ .    # for hadoop
	#cp knn/test/* -r test

lib: disc collections

disc:
	javac -classpath util.jar -g com/mellowtech/disc/*.java com/mellowtech/disc/sort/*.java
	jar -cvf disc.jar com/mellowtech/disc/*.class com/mellowtech/disc/sort/*.class

collections:
	javac -classpath util.jar:disc.jar -g com/mellowtech/collections/*.java
	jar -cvf collections.jar com/mellowtech/collections/*.class

clean:
	rm knn/*.class -rf
	rm knn/test/*.class -rf
