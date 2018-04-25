package jobs;

import constants.CommonConstants;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import util.FileOperation;
import util.InputStreamReaderRunnable;

public class SubmitSparkJob {
    private static final Logger logger = Logger.getLogger(SubmitSparkJob.class);

    public static void submitJob(String jobPath, String mainClass) throws Exception {

        logger.info("Starting submitJob for jar : " + jobPath + " with main class " + mainClass);
        SparkLauncher sparkLauncher = new SparkLauncher()
                .setSparkHome(CommonConstants.SPARK_HOME)
                .setAppResource(jobPath)
                .setMainClass(mainClass)
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
