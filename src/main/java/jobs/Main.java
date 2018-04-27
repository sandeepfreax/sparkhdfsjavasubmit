package jobs;

import constants.CommonConstants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import util.ConfigurationManager;
import util.FileOperation;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        logger.info("Job started at : " + startTime);

        FileOperation fileOperation = new FileOperation();

        FileSystem fileSystem = FileSystem.get(ConfigurationManager.getConfiguration());

        fileOperation.checkAndSubmitJob(fileSystem,
                CommonConstants.PATH_TO_MONITOR_FIRST,
                CommonConstants.FIRST_JOB_PATH,
                CommonConstants.MAIN_CLASS_FIRST_JOB);

        fileOperation.moveFilesSourceToDestination(fileSystem,
                CommonConstants.SOURCE_FOLDER_JOB_ONE,
                CommonConstants.DESTINATION_FOLDER_JOB_ONE);

        long endTime = System.currentTimeMillis() - startTime;
        logger.info("Job ended at : " + endTime);
        logger.info("Job took : " + endTime/1000 + " seconds");
    }
}
