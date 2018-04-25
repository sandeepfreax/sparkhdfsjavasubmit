package jobs;

import constants.CommonConstants;
import org.apache.log4j.Logger;
import util.FileOperation;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        logger.info("Job started at : " + startTime);
        FileOperation fileOperation = new FileOperation();
        fileOperation.checkAndSubmitJob(CommonConstants.PATH_TO_MONITOR_FIRST, CommonConstants.FIRST_JOB_PATH, CommonConstants.MAIN_CLASS_FIRST_JOB);
        long endTime = System.currentTimeMillis() - startTime;
        logger.info("Job ended at : " + endTime);
        logger.info("Job took : " + endTime/1000 + " seconds");
    }
}
