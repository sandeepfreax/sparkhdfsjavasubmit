package util;

import constants.CommonConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class ConfigurationManager {

    private static final Logger logger = Logger.getLogger(ConfigurationManager.class);

    private static Configuration configuration;

    public static Configuration getConfiguration() throws IOException {
        if(configuration == null){
            configuration = new Configuration();
        }

        configuration.addResource(new File(CommonConstants.HADOOP_CONF_CORE_SITE).getAbsoluteFile().toURI().toURL());
        configuration.addResource(new File(CommonConstants.HADOOP_CONF_HDFS_SITE).getAbsoluteFile().toURI().toURL());
        configuration.addResource(new File(CommonConstants.HADOOP_CONF_MAPRED_SITE).getAbsoluteFile().toURI().toURL());
        configuration.addResource(new File(CommonConstants.HADOOP_CONF_YARN_SITE).getAbsoluteFile().toURI().toURL());

        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("hadoop.home.dir", CommonConstants.HADOOP_HOME);
        configuration.set("hadoop.conf.dir", CommonConstants.HADOOP_CONF_DIR);
        configuration.set("yarn.conf.dir", CommonConstants.HADOOP_CONF_DIR);

        return configuration;
    }
}
