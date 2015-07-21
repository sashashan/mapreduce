HADOOP_VERSION=2.6.0
HADOOP_HOME=/u/qqiu/hadoop-${HADOOP_VERSION}/share/hadoop
HADOOP_HOME=/u/qqiu/hadoop-${HADOOP_VERSION}
JLIBS1=${HADOOP_HOME}/etc/hadoop
JLIBS2=${HADOOP_HOME}/common/lib/*
JLIBS3=${HADOOP_HOME}/common/*
JLIBS4=${HADOOP_HOME}/hdfs/lib/*
JLIBS5=${HADOOP_HOME}/hdfs/*
JLIBS6=${HADOOP_HOME}/yarn/lib/*
JLIBS7=${HADOOP_HOME}/yarn/*
JLIBS8=${HADOOP_HOME}/mapreduce/lib/*
JLIBS9=${HADOOP_HOME}/mapreduce/*
JLIBS10=${HADOOP_HOME}/contrib/capacity-scheduler/*
JLIBS11=${HADOOP_HOME}/tool/lib/*
JLIBS12=${HADOOP_HOME}/tool/*
LIBDIR=lib

MLIBS=${libjars}
MLIBS=elki.jar
JPATH=~/lib/jdk1.7.0_79/bin#OPENJDK might not be able to compile the elki.jar, use sun JDK instead in this case
JC=${JPATH}/javac


all:
	rm -rf knn
	mkdir knn
	${JC} -classpath /u/qqiu/hadoop-2.6.0/:${MLIBS} -d knn *.java
	jar -cvf ./knn.jar -C knn/ .     # for hadoop
	#cp knn/test/* -r test

bulkload:  
	${JC} -g  de/lmu/ifi/dbs/elki/index/tree/spatial/rstarvariants/NonFlatRStarTree.java
	${JC} -g  de/lmu/ifi/dbs/elki/index/Zorder.java
	${JC} -g  de/lmu/ifi/dbs/elki/index/ExternalSort.java
	jar -uvf elki.jar \
	de/lmu/ifi/dbs/elki/index/Zorder*.class \
	de/lmu/ifi/dbs/elki/index/ExternalSort*.class
	#de/lmu/ifi/dbs/elki/index/tree/spatial/rstarvariants/Non*.class

#build elki.jar 
otherjar=${LIBDIR}/*
#org.w3c.dom.svg_1.1.0.v200806040011.jar:${LIBDIR}/batik-all-1.7.jar:${LIBDIR}/commons-math-1.2.jar:${LIBDIR}/fop.jar:${LIBDIR}/batik-all-1.7.jar

elki:
	${JC} -classpath ${otherjar}:./ -Xlint:deprecation de/lmu/ifi/dbs/elki/*.java
	${JC} -classpath ${otherjar}:./ -Xlint:deprecation de/lmu/ifi/dbs/elki/*/*.java
	${JC} -classpath ${otherjar}:./ -Xlint:deprecation de/lmu/ifi/dbs/elki/*/*/*.java
	${JC} -classpath ${otherjar}:./ -Xlint:deprecation de/lmu/ifi/dbs/elki/*/*/*/*.java
	${JC} -classpath ${otherjar}:./ -Xlint:deprecation de/lmu/ifi/dbs/elki/*/*/*/*/*.java
	${JC} -classpath ${otherjar}:./ -Xlint:deprecation de/lmu/ifi/dbs/elki/*/*/*/*/*/*.java
	${JC} -classpath ${otherjar}:./ -Xlint:deprecation de/lmu/ifi/dbs/elki/*/*/*/*/*/*/*.java
	${JPATH}/jar -cvf elki.jar \
	de/lmu/ifi/dbs/elki/*.class  \
	de/lmu/ifi/dbs/elki/*/*.class \
	de/lmu/ifi/dbs/elki/*/*/*.class \
	de/lmu/ifi/dbs/elki/*/*/*/*.class \
	de/lmu/ifi/dbs/elki/*/*/*/*/*.class \
	de/lmu/ifi/dbs/elki/*/*/*/*/*/*.class \
	de/lmu/ifi/dbs/elki/*/*/*/*/*/*/*.class \
	de/lmu/ifi/dbs/elki/logging/logging-cli.properties
	
clean:
	rm -rf knn
	rm -rf knn.jar
	rm -rf *.class
	#rm elki.jar

