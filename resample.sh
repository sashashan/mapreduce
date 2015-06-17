<<OVERVIEW
	This is a simple scipt that allows you to change the input sample files faster than what you would
	have to do manually. The "outer" (R points) and "inner" (S points) files that you can change can be found
	in /mrknnj-release/hbrj directroy. 
	(The origianl sample file provided by the writers is inside the 
	/mrknnj-release/hbrj/safe directory.)
	After changing those files simply run this script and it will change the correct files inside the hadoop 
	system for you. 
OVERVIEW
#cp mrknnj-release/hbrj/input/sample_outer_r /usr/local/hadoop-2.6.0/hbrj/input/r
#cp mrknnj-release/hbrj/input/sample_inner_s /usr/local/hadoop-2.6.0/hbrj/input/s

#/usr/local/hadoop-2.6.0/bin/hdfs namenode -format
#/usr/local/hadoop-2.6.0/sbin/start-dfs.sh
#/usr/local/hadoop-2.6.0/bin/hdfs dfs -mkdir /user/
#/usr/local/hadoop-2.6.0/bin/hdfs dfs -mkdir /user/sasha
#/usr/local/hadoop-2.6.0/bin/hdfs dfs -mkdir /user/sasha/hbrj
#/usr/local/hadoop-2.6.0/bin/hdfs dfs -mkdir /user/sasha/hbrj/r
#/usr/local/hadoop-2.6.0/bin/hdfs dfs -mkdir /user/sasha/hbrj/s

/usr/local/hadoop-2.6.0/bin/hdfs dfs -rm /user/sasha/hbrj/r/sample_outer_r
/usr/local/hadoop-2.6.0/bin/hdfs dfs -rm /user/sasha/hbrj/s/sample_inner_s

/usr/local/hadoop-2.6.0/bin/hdfs dfs -put ~/mapreduce/mrknnj-release/hbrj/input/sample_outer_r /user/sasha/hbrj/r
/usr/local/hadoop-2.6.0/bin/hdfs dfs -put ~/mapreduce/mrknnj-release/hbrj/input/sample_inner_s /user/sasha/hbrj/s