The hadoop based block R-tree KNN join algorithm (H-BRJ) 
consists of 2-round MapReduce phases and the corresponding source 
files to each stage are given as follows:
Round1: RPhase1.java  RPhase1Key.java  RPhase1Value.java Zorder.java
Round2: RPhase2.java  RPhase2Value.java 

To compile these files, a third party library is required. We include the
library named as rtree.tar.bz2 in the tar file.

The library can also be found in ,the following webpage: 
http://elki.dbs.ifi.lmu.de/wiki/Releases
Please download elik release 0.3 and apply the patch file (patch-index) properly.
To compile elki package, you need to download the following jar files and put them
in lib directory.
######
batik-all-1.7.jar
commons-math-1.2.jar  
fop.jar  
org.w3c.dom.svg_1.1.0.v200806040011.jar
######

A Makefile is provided for compiling the code and the library.
You may need to modify the Makefile to make it work on your own system.
make all  # compile the source code related to H-BRJ algorithm.
make elki # to compile related libraries 

An complete example of running the programs are given as follows:

Round 1:
hadoop jar knn.jar test.RPhase1 -m 1 -r 16 -p 4 -d 2 -k 10 -b 100000 c16/rsr40m c16/rss40m phase1out

-m: specify the number of mappers 
-r: specify the number of reducers
-p: specify the number of partitions/buckets
-d: specify the dimensionality of the input datasets
-k: specify the number of the nearest neighbors to be retrieved
-b: specify the buffer size

In this case, input data sets are put in HDFS directory c16/rsr40m and 
c16/rss40m. The output datasets are under phase1out.

Round 2:
hadoop jar knn.jar test.RPhase2 -m 1 -r 16 -k 10 phase1out phase2out

-m: specify the number of mappers (should set to the number of splits)
-r: specify the number of reduces (decided by values from -s and -p)
-k: specify the number of the nearest neighbors to be retrieved
-p: specify the number of partitions/buckets

In this case, input datasets reside on phase1out and output datasets are
saved in phase2out.

If you have any questions, please send email to us.
