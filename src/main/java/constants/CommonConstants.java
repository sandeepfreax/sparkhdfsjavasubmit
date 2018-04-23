package constants;

public class CommonConstants {
    public static final String HADOOP_HOME = "C:\\bigdata\\configFiles\\hadoop-2.7.3";
    public static final String HADOOP_CONF_DIR = HADOOP_HOME + "\\etc\\hadoop";
    public static final String HADOOP_CONF_CORE_SITE = HADOOP_CONF_DIR + "\\core-site.xml";
    public static final String HADOOP_CONF_HDFS_SITE = HADOOP_CONF_DIR + "\\hdfs-site.xml";
    public static final String HADOOP_CONF_MAPRED_SITE = HADOOP_CONF_DIR + "\\mapred-site.xml";
    public static final String HADOOP_CONF_YARN_SITE = HADOOP_CONF_DIR + "\\yarn-site.xml";

    public static final String JAVA_HOME = "C:\\Program Files\\Java\\jdk1.8.0_92";
    public static final String SPARK_HOME = "C:\\bigdata\\configFiles\\spark-2.2.0\\";
    public static final String FIRST_JOB_PATH = "/testJars/simplespark_2.12-0.1.jar";
    public static final String MAIN_CLASS_FIRST_JOB = "SimpleApp";
    public static final String[] FIRST_JOB_ARGUMENTS = new String[]{"Hello World!!!"};
    public static final String MASTER_NAME = "local[*]";
    public static final String DRIVER_MEMORY_FIRST_JOB = "2g";
}
