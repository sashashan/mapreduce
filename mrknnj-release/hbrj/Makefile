MLIBS=elki.jar
#OPENJDK might not be able to compile the elki.jar, use sun JDK instead in this case
#JPATH=/usr/lib/jvm/java-1.7.0/bin
JPATH=~/usr/local/jdk1.7.0_79/bin
JC=${JPATH}/javac
LIBDIR=./lib
otherjar=${LIBDIR}/*

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
	rm elki.jar
