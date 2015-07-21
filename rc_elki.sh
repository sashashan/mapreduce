<<OVERVIEW
    This script deletes the old elki.jar and recompiles it using the updated .java files which you should
    change inside mrknnj-release/hbrj/de directory. The original source files given by the authors can be found
    inside mrknnj-release/rtree/src directory. If you ever need to go back to the original state, copy src/de
    into mrknnj-release/hbrj and replace with the old de foulder.
OVERVIEW

rm mrknnj-release/hbrj/elki.jar
cd mrknnj-release/hbrj
make elki
cd ~/workspace/mapreduce
