The hadoop based block nested loop KNN join algorithm (H-BNLJ) 
consists of 2-round MapReduce phases and the corresponding source 
files to each stage are given as follows:
Round1: NPhase1.java  NPhase1Value.java ListElem.java RecordComparator.java
Round2: NPhase2.java  NPhase2Value.java 

A Makefile is provided for compiling the codes.
You may need to modify the Makefile to make it work on your own system.
make all  # compile the source code related to H-BRJ algorithm.
make clean # clean .class and .tar files

An complete example of running the programs are given as follows:

Round 1:
hadoop jar knn.jar test.NPhase1 -m 1 -r 16 -p 4 -d 2 -k 10 -b 100000 c16/rsr40m c16/rss40m phase1out

-m: specify the number of mappers (should set to the number of splits)
-r: specify the number of reducers (should set to the number of random shift copies)
-p: specify the number of partitions/buckets
-d: specify the dimensionality of the input datasets
-k: specify the number of the nearest neighbors to be retrieved
-b: specify the buffer size (bytes)

In this case, input data sets are put in HDFS directory c16/rsr40m and 
c16/rss40m. The output datasets are under phase1out.

Round 2:
hadoop jar knn.jar test.NPhase2 -m 1 -r 16 -k 10 phase1out phase2out

-m: specify the number of mappers (should set to the number of splits)
-r: specify the number of reduces (decided by values from -s and -p)
-k: specify the number of the nearest neighbors to be retrieved
-p: specify the number of partitions/buckets

In this case, input datasets reside on phase1out and output datasets are
saved in phase2out.

If you have any questions, please send email to us.
