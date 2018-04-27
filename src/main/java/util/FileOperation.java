package util;

import constants.CommonConstants;
import constants.ConfigConstants;
import jobs.SubmitSparkJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * This class will faciliate with different utilities that interact with HDFS
 * */
public class FileOperation {

    private static Logger logger = Logger.getLogger(FileOperation.class);

    /*
    * This method check the size of a directory at configured time interval. If it finds size upto a configured size,
    * it will trigger a Spark job, if threshold for folder size is not met then it will wait for certian time and
    * then submits Spark Job on cluster
    * */
    public void checkAndSubmitJob(FileSystem fileSystem,
                                  String dirToMonitor,
                                  String sparkJarPath,
                                  String sparkJarMainClass) {

        logger.info("Starting thread to monitor changes in dir : " + dirToMonitor);
        InputStream inputStream = null;

        try{
            Path dirName = new Path(dirToMonitor);

            Path configFilePath = new Path(CommonConstants.PATH_TO_CONFIG_FILE);
            inputStream = fileSystem.open(configFilePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            Map<String, String> configMap = FileUtils.getMapFromConfigFile(br);

            int thresholdDirSize =  Integer.valueOf(configMap.get(ConfigConstants.THRESHOLD_DIR_SIZE));
            logger.info("Threshold DIR size : " + thresholdDirSize);
            int thresholdWaitTimeLimit = Integer.valueOf(configMap.get(ConfigConstants.THRESHOLD_TIME_LIMIT_MIN));
            logger.info("Threshold Wait time : " + thresholdWaitTimeLimit);
            int sleepTime = Integer.valueOf(configMap.get(ConfigConstants.SLEEP_INTERVAL_SECONDS));
            logger.info("Sleep time : " + sleepTime);

            int stepThreshold = (thresholdWaitTimeLimit * 60) / sleepTime;
            int step = 0;
            boolean flag = true;
            double dirSize;

            while (flag) {
                dirSize = fileSystem.getContentSummary(dirName).getLength()/Math.pow(2, 30);
                logger.info("Size of dir " + dirToMonitor + " : " + dirSize + " GB.");
                if(dirSize >= thresholdDirSize || step == stepThreshold){
                    SubmitSparkJob.submitJob(sparkJarPath, sparkJarMainClass);
                    step = 0;
                    flag = false;           //comment this code to run it for infinite loop
                }else {
                    Thread.sleep(sleepTime*1000);
                    step++;
                }
            }
        }catch (Exception exception){
            logger.error("Caught exception while monitoring the changes in " +
                    CommonConstants.PATH_TO_MONITOR_FIRST, exception);
        }finally {
            InputOutputUtil.closeInputStream(inputStream);
        }
    }

    /*
    * This method moves all the files from one folder to another on HDFS.
    * */
    public void moveFilesSourceToDestination(FileSystem fileSystem,
                                             String sourceDir,
                                             String targetDir) throws IOException{
        logger.info("Source files are under : " + sourceDir);
        logger.info("Destination path to these files : " + targetDir);
        Path sourcePath = new Path(sourceDir);
        Path destinationPath = new Path(targetDir);

        FileStatus[] fileStatuses = fileSystem.listStatus(sourcePath);
        if(fileStatuses.length > 0){
            logger.info("Started copying the files");
            for (FileStatus fileStatus : fileStatuses){
                FileUtil.copy(fileSystem,
                        fileStatus.getPath(),
                        fileSystem,
                        destinationPath,
                        true,
                        ConfigurationManager.getConfiguration());
            }
            logger.info("Files moved successfully.");
        } else {
            logger.info("Source Directory is empty.");
        }
    }
}
