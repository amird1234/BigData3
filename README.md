Locally:
=======
./bin/spark-submit   --class univ.bigdata.course.MainRunner   --master local[*]   --deploy-mode client   final-project-1.0-SNAPSHOT.jar

In the cluster:
=============
./bin/spark-submit --class univ.bigdata.course.MainRunner --master yarn --deploy-mode cluster /tmp/final-project-1.0-SNAPSHOT.jar /tmp/movies-simple.txt
  
SparkPi on Cluster
==================
./bin/spark-submit --class org.apache.spark.examples.SparkPi --master yarn --deploy-mode cluster --executor-memory 20G --num-executors 50 /path/to/examples.jar 1000

