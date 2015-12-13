# HDFS Commands

#### list a directory
bash> hdfs dfs -ls <HDFS_PATH>

#### create a directory
bash> hdfs dfs -mkdir [-p] <HDFS_PATH>

#### put a file in path
bash> hdfs dfs -put <SRC> <HDFS_TARGET>

#### get a file in path
bash> hdfs dfs -get <HDFS_SRC> [LOCAL_TARGET]