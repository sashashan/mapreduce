The hadoop based zvalue KNN join algorithm (H-zKNNJ) 
consists of 3-round MapReduce phases and the corresponding source 
files to each stage are given as follows:
Round1: BPhase1.java TextBytePair.java BPhase1Value.java KnnRecordComparator.java  KnnRecord.java
Round2: BPhase2.java BPhase2Key.java BPhase2Value.java 
Round3: BPhase3.java BPhase3Value.java

To compile these file, a B+ tree library is required. We include the library
package name as btree.tar.bz2 in this tar file. The library can be also be found 
in the following link: http://www.sics.se/humle/socialcomputing/download/
You need to download three package Collections (Version 0.9), Disc (Version 0.9), 
and Util. Next, you can apply our patch files patch-collections and patch-disc
to Collections and Disc packages.

A Makefile is provided for compiling the code and the library.
You may need to modify the Makefile to make it work on your own system.
make all  # compile the source code related to H-zKNNJ algorithm.
make libs # compile related libraries 

An complete example of running the programs are given as follows:

Round 1:
hadoop jar knn.jar test.BPhase1 -m 1 -r 16 -s 2 -p 8 -nr 200000 -ns 40000000 -e 0.003 -d 2 -k 10 -pr true -c c18 -outer c20/data/rsr40m-200k -inner c18/data/rsr40m -o phase1out

-m: specify the number of mappers (should set to the number of splits)
-r: specify the number of reducers (should set to the number of random shift copies)
-s: specify the number of random shift copies
-p: specify the number of partitions for each random shift copy
-nr: specify the number of records in R (the outer dataset)
-ns: specify the number of records in S (the inner dataset)
-e: specify the epsilon value, which is used to decide the sampling rate
-d: specify the dimensionality of the input datasets
-k: specify the number of the nearest neighbors to be retrieved
-pr: specify partition on R(true) or S(false)
-c: specify the HDFS directoy used for distributed cache
-outer: specify the HDFS directory containing R (the dataset must be named as outer)
-inner: specify the HDFS directory containing S (the dataset must be named as inner)
-o: specify the HDFS output directory as output_dir

In this case, input data sets are put in HDFS directory c20/data/rsr40m-200k and 
c20/data/rsr40m. The output datasets are under phase1out.

Round 2:
hadoop fs -mkdir c20/range-rsr40m-200k-rsr40m-10
hadoop fs -mv phase1out/Rrange0* c20/range-rsr40m-200k-rsr40m-10/Rrange0 
hadoop fs -mv phase1out/Rrange1* c20/range-rsr40m-200k-rsr40m-10/Rrange1
hadoop fs -mv phase1out/Srange0* c20/range-rsr40m-200k-rsr40m-10/Srange0 
hadoop fs -mv phase1out/Srange1* c20/range-rsr40m-200k-rsr40m-10/Srange1

#The above commands are to move partitioning ranges to c20 (the distributed cache).

hadoop fs -rm phase1out/part* 
hadoop fs -rm phase1out/_logs*
#The above commands are to remove unnecessary files.

hadoop jar knn.jar test.BPhase2 -libjars disc.jar,collections.jar -m 1 -r 16 -s 2 -p 8 -d 2 -k 10 -c c20 -outer rsr40m-200k -inner rsr40m phase1out phase2out

-m: specify the number of mappers (should set to the number of splits)
-r: specify the number of reduces (decided by values from -s and -p)
-s: specify the number of random shift copies
-p: specify the number of partitions for each random shift copy
-d: specify the dimensionality of the input datasets
-k: specify the number of the nearest neighbors to be retrieved
-c: specify the HDFS directory used for distributed cache
-outer: specify the name to represent R 
-inner: specify the name to represent S 

In this case, input datasets reside on phase1out and output datasets are saved
in phase1out. 

Round 3:
hadoop jar knn.jar test.BPhase3 -m 1 -r 16 -k 10 phase2out phase3out

-m: specify the number of mappers (should set to the number of splits)
-r: specify the number of reduces (decided by values from -s and -p)
-k: specify the number of the nearest neighbors to be retrieved

In this case, input datasets reside on phase2out and output datasets are
saved in phase2out.

If you have any questions, please send email to us.
