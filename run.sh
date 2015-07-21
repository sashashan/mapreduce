hadoop fs -rm -r /user/qqiu/hbrj/output
hadoop jar build/Hadoop.jar test.RPhase1 -m 1 -p 3 -d 2 -k 2 -b 10 hbrj/input/r/$1 hbrj/input/s/$2 hbrj/output
