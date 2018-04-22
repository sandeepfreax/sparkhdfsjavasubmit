package util;

import constants.CommonConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class ConfigurationManager {

    private static final Logger logger = Logger.getLogger(ConfigurationManager.class);

    private static ConfigurationManager configurationManager;

    private static final String HADOOP_HOME = CommonConstants.HADOOP_HOME;
    private static final String HADOOP_CONF_DIR = HADOOP_HOME + "\\etc\\hadoop";
    private static final String HADOOP_CONF_CORE_SITE = HADOOP_CONF_DIR + "\\core-site.xml";
    private static final String HADOOP_CONF_HDFS_SITE = HADOOP_CONF_DIR + "\\hdfs-site.xml";
    private static final String HADOOP_CONF_MAPRED_SITE = HADOOP_CONF_DIR + "\\mapred-site.xml";
    private static final String HADOOP_CONF_YARN_SITE = HADOOP_CONF_DIR + "\\yarn-site.xml";

    private static Configuration getConfiguration() throws IOException {
        logger.info("Hadoop home directory : " + HADOOP_HOME);

        Configuration configuration = new Configuration();

        configuration.addResource(new File(HADOOP_CONF_CORE_SITE).getAbsoluteFile().toURI().toURL());
        configuration.addResource(new File(HADOOP_CONF_HDFS_SITE).getAbsoluteFile().toURI().toURL());
        configuration.addResource(new File(HADOOP_CONF_MAPRED_SITE).getAbsoluteFile().toURI().toURL());
        configuration.addResource(new File(HADOOP_CONF_YARN_SITE).getAbsoluteFile().toURI().toURL());

        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("hadoop.home.dir", HADOOP_HOME);
        configuration.set("hadoop.conf.dir", HADOOP_CONF_DIR);
        configuration.set("yarn.conf.dir", HADOOP_CONF_DIR);

        return configuration;
    }

    public static ConfigurationManager getInstance() {
        if(configurationManager == null){
            configurationManager = new ConfigurationManager();
        }
        return configurationManager;
    }

}
