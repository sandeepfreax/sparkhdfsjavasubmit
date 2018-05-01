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

#Configuration
1. Keep a .conf file at a specific location
2. In this conf, include following parameters that are needed per job
    a. Job_path => spark job path that will be triggered
    b. main_class => main class of this spark application that is to be ivoked
    c. master_name => name of spark master job
    d. path_to_monitor => path of dir whose size will be monitored to trigger spark job
    e. source_folder => source folder from where files will be transferred
    f. destination_folder => destination folder where files will land
    g. info_log_dir => directory for info logs of spark job
    h. error_log_dir => directory for error logs of spark job
    i. threshold_dir_size => threshold directory size at which jobs will trigger (in GB)
    j. threshold_time_limit_min => threshold time that will eventually trigger the job if directory size has not reached threshold value (in min)
    k. sleep_time => sleep time to check size of directory again
3. In .conf file, all keys shall be uniue so kindly name them as per your convenience for your jobs.
4. This .conf file also contains the spark_home that holds value for spark home
5. Entries made in .conf shall be reflected in CommonConstants.java to avoid any typo or to use them further in application.
6. In main class, instantiate threads with corresponding values and start the threads.
7. Prerequisite for this application is that all the paths mentioned in .conf file should exist before running the module.

To run this sample application pass path of configuration file while invoking the application.
e.g.
hadoop jar sparkJobSubmit-1.0.0.jar jobs.Main /data/config/sparkApplication.conf