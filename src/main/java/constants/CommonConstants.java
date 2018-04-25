package constants;

public class CommonConstants {
    public static final String SPARK_HOME = "C:\\bigdata\\configFiles\\spark-2.2.0";
    public static final String FIRST_JOB_PATH = "C:\\bigdata\\configFiles\\spark-2.2.0\\examples\\jars\\spark-examples_2.11-2.2.0.jar";
    public static final String MAIN_CLASS_FIRST_JOB = "org.apache.spark.examples.SparkPi";
    public static final String MASTER_NAME = "local[*]";

    public static final String HADOOP_HOME = "C:\\bigdata\\configFiles\\hadoop-2.7.3";
    public static final String HADOOP_CONF_DIR = HADOOP_HOME + "\\etc\\hadoop";
    public static final String HADOOP_CONF_CORE_SITE = HADOOP_CONF_DIR + "\\core-site.xml";
    public static final String HADOOP_CONF_HDFS_SITE = HADOOP_CONF_DIR + "\\hdfs-site.xml";
    public static final String HADOOP_CONF_MAPRED_SITE = HADOOP_CONF_DIR + "\\mapred-site.xml";
    public static final String HADOOP_CONF_YARN_SITE = HADOOP_CONF_DIR + "\\yarn-site.xml";

    public static final String PATH_TO_MONITOR_FIRST = "/testJars";
    public static final String PATH_TO_CONFIG_FILE = "/data/config/sparkSubmitJava.conf";
}
