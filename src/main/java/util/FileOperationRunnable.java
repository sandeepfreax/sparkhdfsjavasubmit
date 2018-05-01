package util;

import jobs.SubmitSparkJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class will faciliate with different utilities that interact with HDFS
 * */
public class FileOperationRunnable implements Runnable{

    private static Logger logger = Logger.getLogger(FileOperationRunnable.class);

    private FileSystem fileSystem;
    private String dirToMonitor;
    private String sparkJarPath;
    private String sparkJarMainClass;
    private String thresholdSize;
    private String thresholdWaitTime;
    private String waitingTime;
    private String sourceFileDir;
    private String targetFileDir;
    private String sparkHome;
    private String firstJobMasterName;

    public FileOperationRunnable(FileSystem fileSystem,
                                 String sparkHome,
                                 String dirToMonitor,
                                 String sparkJarPath,
                                 String sparkJarMainClass,
                                 String firstJobMasterName,
                                 String thresholdSize,
                                 String thresholdWaitTime,
                                 String waitingTime,
                                 String sourceFileDir,
                                 String targetFileDir) {
        logger.info("Initialising FileOperationRunnable for : " + sparkJarMainClass);
        this.sparkHome = sparkHome;
        this.fileSystem = fileSystem;
        this.dirToMonitor = dirToMonitor;
        this.sparkJarPath = sparkJarPath;
        this.sparkJarMainClass = sparkJarMainClass;
        this.firstJobMasterName = firstJobMasterName;
        this.thresholdSize = thresholdSize;
        this.thresholdWaitTime = thresholdWaitTime;
        this.waitingTime = waitingTime;
        this.sourceFileDir = sourceFileDir;
        this.targetFileDir = targetFileDir;
    }

    /*
    * This method check the size of a directory at configured time interval. If it finds size upto a configured size,
    * it will trigger a Spark job, if threshold for folder size is not met then it will wait for certian time and
    * then submits Spark Job on cluster
    * */
    public void run() {

        logger.info("Starting thread to monitor changes in dir : " + dirToMonitor);
        InputStream inputStream = null;

        try{
            Path dirName = new Path(dirToMonitor);

            int thresholdDirSize =  Integer.valueOf(thresholdSize);
            logger.info("Threshold DIR size : " + thresholdDirSize);
            int thresholdWaitTimeLimit = Integer.valueOf(thresholdWaitTime);
            logger.info("Threshold Wait time : " + thresholdWaitTimeLimit);
            int sleepTime = Integer.valueOf(waitingTime);
            logger.info("Sleep time : " + sleepTime);

            int stepThreshold = (thresholdWaitTimeLimit * 60) / sleepTime;
            int step = 0;
            boolean flag = true;
            double dirSize;

            while (flag) {
                dirSize = fileSystem.getContentSummary(dirName).getLength()/Math.pow(2, 30);
                logger.info("Size of dir " + dirToMonitor + " : " + dirSize + " GB.");
                if(dirSize >= thresholdDirSize || step == stepThreshold){
                    SubmitSparkJob.submitJob(sparkHome, firstJobMasterName, sparkJarPath, sparkJarMainClass);
                    step = 0;
                    moveFilesSourceToDestination(fileSystem, sourceFileDir, targetFileDir);
                    flag = false;           //comment this code to run it for infinite loop
                }else {
                    Thread.sleep(sleepTime*1000);
                    step++;
                }
            }
        }catch (Exception exception){
            logger.error("Caught exception while monitoring the changes in " +
                    dirToMonitor, exception);
        }finally {
            InputOutputUtil.closeInputStream(inputStream);
        }
    }

    /*
    * This method moves all the files from one folder to another on HDFS.
    * */
    private void moveFilesSourceToDestination(FileSystem fileSystem,
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
