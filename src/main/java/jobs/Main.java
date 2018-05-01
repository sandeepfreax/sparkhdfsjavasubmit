package jobs;

import constants.CommonConstants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import util.ConfigurationManager;
import util.FileOperationRunnable;
import util.FileUtils;

import java.util.Map;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        logger.info("Starting the application.");
        if(args.length > 1){
            String configLoc = args[1];
            logger.info("Path to config file is : " + configLoc);
            FileSystem fileSystem = FileSystem.get(ConfigurationManager.getConfiguration());
            Map<String, String> configMap = FileUtils.getMapFromConfigFile(fileSystem, configLoc);
            String sparkHome = configMap.get(CommonConstants.SPARK_HOME);

            //get required configuration needed to run first job
            String pathToMonitorFirst = configMap.get(CommonConstants.PATH_TO_MONITOR_FIRST);
            String firstJobPath = configMap.get(CommonConstants.FIRST_JOB_PATH);
            String firstJobMainClass = configMap.get(CommonConstants.FIRST_JOB_MAIN_CLASS);
            String thresholdDirSizeFirstJob = configMap.get(CommonConstants.THRESHOLD_DIR_SIZE_GB_FIRST_JOB);
            String thresholdTimeLimitMinFirstJob = configMap.get(CommonConstants.THRESHOLD_TIME_LIMIT_MIN_FIRST_JOB);
            String sleepIntervalSecondsFirstJob = configMap.get(CommonConstants.SLEEP_INTERVAL_SEC_FIRST_JOB);
            String sourceFolderJobOne = configMap.get(CommonConstants.SOURCE_FOLDER_JOB_ONE);
            String destinationFolderJobOne = configMap.get(CommonConstants.DESTINATION_FOLDER_JOB_ONE);
            String firstJobMasterName = configMap.get(CommonConstants.FIRST_JOB_MASTER_NAME);
            String infoLogDirFirstJob = configMap.get(CommonConstants.INFO_LOG_DIR_FIRST_JOB);
            String errorLogDirFirstJob = configMap.get(CommonConstants.ERROR_LOG_DIR_FIRST_JOB);

            FileOperationRunnable fileOperationRunnable = new FileOperationRunnable(fileSystem,
                    sparkHome,
                    pathToMonitorFirst,
                    firstJobPath,
                    firstJobMainClass,
                    firstJobMasterName,
                    thresholdDirSizeFirstJob,
                    thresholdTimeLimitMinFirstJob,
                    sleepIntervalSecondsFirstJob,
                    sourceFolderJobOne,
                    destinationFolderJobOne,
                    infoLogDirFirstJob,
                    errorLogDirFirstJob);

            Thread firstJob = new Thread(fileOperationRunnable);
            firstJob.start();


            //get required configuration needed to run second job
            String pathToMonitorSecond = configMap.get(CommonConstants.PATH_TO_MONITOR_FIRST);
            String secondJobPath = configMap.get(CommonConstants.FIRST_JOB_PATH);
            String secondJobMainClass = configMap.get(CommonConstants.FIRST_JOB_MAIN_CLASS);
            String thresholdDirSizeSecondJob = configMap.get(CommonConstants.THRESHOLD_DIR_SIZE_GB_FIRST_JOB);
            String thresholdTimeLimitMinSecondJob = configMap.get(CommonConstants.THRESHOLD_TIME_LIMIT_MIN_FIRST_JOB);
            String sleepIntervalSecondsSecondJob = configMap.get(CommonConstants.SLEEP_INTERVAL_SEC_FIRST_JOB);
            String sourceFolderJobTwo = configMap.get(CommonConstants.SOURCE_FOLDER_JOB_ONE);
            String destinationFolderJobTwo = configMap.get(CommonConstants.DESTINATION_FOLDER_JOB_ONE);
            String secondJobMasterName = configMap.get(CommonConstants.FIRST_JOB_MASTER_NAME);
            String infoLogDirSecondJob = configMap.get(CommonConstants.INFO_LOG_DIR_FIRST_JOB);
            String errorLogDirSecondJob = configMap.get(CommonConstants.ERROR_LOG_DIR_FIRST_JOB);

            FileOperationRunnable fileOperationRunnableSecond = new FileOperationRunnable(fileSystem,
                    sparkHome,
                    pathToMonitorSecond,
                    secondJobPath,
                    secondJobMainClass,
                    secondJobMasterName,
                    thresholdDirSizeSecondJob,
                    thresholdTimeLimitMinSecondJob,
                    sleepIntervalSecondsSecondJob,
                    sourceFolderJobTwo,
                    destinationFolderJobTwo,
                    infoLogDirSecondJob,
                    errorLogDirSecondJob);

            Thread secondJob = new Thread(fileOperationRunnableSecond);
            secondJob.start();
        }else {
            logger.info("Insufficient no. of arguments passed to invoke the application. Please pass path for configuration file.");
        }
    }
}
