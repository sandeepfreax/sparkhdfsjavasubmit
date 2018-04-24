package jobs;

import constants.CommonConstants;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import util.InputStreamReaderRunnable;

public class SubmitSparkJob {
    private static final Logger logger = Logger.getLogger(SubmitSparkJob.class);

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        logger.info("Job started at : " + startTime);
        submitJob();
        long endTime = System.currentTimeMillis() - startTime;
        logger.info("Job ended at : " + endTime);
        logger.info("Job took : " + endTime/1000 + " seconds");
    }

    private static void submitJob() throws Exception {
        logger.info("Starting submitJob");
        SparkLauncher sparkLauncher = new SparkLauncher()
                .setSparkHome(CommonConstants.SPARK_HOME)
                .setAppResource(CommonConstants.FIRST_JOB_PATH)
                .setMainClass(CommonConstants.MAIN_CLASS_FIRST_JOB)
                .setMaster(CommonConstants.MASTER_NAME);

        logger.info("Launching the spark application");
        Process process = sparkLauncher.launch();

        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();


        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader input");
        errorThread.start();

        logger.info("Spark Application launched successfully. Waiting for exit code.");
        int exitCode = process.waitFor();
        logger.info("Job finished with exit status : " + exitCode);
    }
}
