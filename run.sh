#rm -r /usr/local/hadoop-2.6.0/hbrj/output
#/usr/local/hadoop-2.6.0/bin/hadoop jar ~/mapreduce/build/Hadoop.jar test.RPhase1 -m 1 -r 6 -p 3 -d 2 -k 2 -b 100000 \
#/usr/local/hadoop-2.6.0/hbrj/input/r /usr/local/hadoop-2.6.0/hbrj/input/s /usr/local/hadoop-2.6.0/hbrj/output

/usr/local/hadoop-2.6.0/bin/hdfs dfs -rm -r /user/sasha/hbrj/output
/usr/local/hadoop-2.6.0/bin/hadoop jar ~/mapreduce/build/Hadoop.jar test.RPhase1 -m 1 -r 6 -p 3 -d 2 -k 2 -b 100000 \
/user/sasha/hbrj/r /user/sasha/hbrj/s /user/sasha/hbrj/output