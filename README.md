# sparkhdfsjavasubmit
Monitor HDFS folder and sumbit a Apache Spark job on cluster

## Project Description
Suppose there are two folders A and B. A folder will be loaded with Employee data files and B folder will be loaded with Department data files.

Every minute, the java program should monitor these folders and trigger SparkA(based on folder A) or SparkB(based on folder B) programs(Spark-Submit or equivalent). After successful completion, the java program should move the processed files to a different location. The condition is either for every five minutes or for exceeding the size of the folder to more than 2GB.(Spark 2.1.1)

## Additional Project Description:

# Task 1:
Run the spark job(spark-submit) from java code.

# Task2:
Schedule the java class based on time or size, whatever matches first. (Configurable size or configurable time)

# Task3:
Java class should read the files in the folder and move them to a different folder and Spark job will read the files from this new folder.