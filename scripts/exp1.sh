
R_SET="r_outer"
S_SET="s_inner"

MAP="-D mapreduce.tasktracker.map.tasks.maximum="
RED="-D mapreduce.tasktracker.reduce.tasks.maximum="

END=4
for i in $(seq 1 $END); 
do 
    for j in $(seq 1 $END);
    do 
        echo $i, $j; 
        sh run.sh $R_SET $S_SET $MAP$i $RED$j
        sh run2.sh $MAP$i $RED$j
    done 
done

