#rm -r /usr/local/hadoop-2.6.0/hbrj/output2
#/usr/local/hadoop-2.6.0/bin/hadoop jar ~/mapreduce/build/Hadoop.jar test.RPhase2 -m 1 -r 6 -k 2 \
#/usr/local/hadoop-2.6.0/hbrj/output /usr/local/hadoop-2.6.0/hbrj/output2

/usr/local/hadoop-2.6.0/bin/hdfs dfs -rm -r /user/sasha/hbrj/output2
/usr/local/hadoop-2.6.0/bin/hadoop jar ~/mapreduce/build/Hadoop.jar test.RPhase2 -m 1 -r 6 -k 2 \
/user/sasha/hbrj/output /user/sasha/hbrj/output2