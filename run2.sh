hadoop fs -rm -r hbrj/output2
hadoop jar build/Hadoop.jar test.RPhase2 -m 1 -r 6 -k 2 \
hbrj/output hbrj/output2

