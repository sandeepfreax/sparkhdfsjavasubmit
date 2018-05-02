package jobs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import util.InputStreamReaderRunnable;

public class SubmitSparkJob {
    private static final Logger logger = Logger.getLogger(SubmitSparkJob.class);

    public static int submitJob(String sparkHome,
                                 String masterName,
                                 String jobPath,
                                 String mainClass,
                                 String infoLogsPath,
                                 String errorLogsPath,
                                 FileSystem fileSystem) throws Exception {

        logger.info("Starting submitJob for jar : " + jobPath + " with main class " + mainClass);
        SparkLauncher sparkLauncher = new SparkLauncher()
                .setSparkHome(sparkHome)
                .setAppResource(jobPath)
                .setMainClass(mainClass)
                .setMaster(masterName);

        logger.info("Launching the spark application");
        Process process = sparkLauncher.launch();

        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(),
                "info",
                infoLogsPath,
                fileSystem);
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();


        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(),
                "error",
                errorLogsPath,
                fileSystem);
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader input");
        errorThread.start();

        logger.info("Spark Application launched successfully. Waiting for exit code.");
        int exitCode = process.waitFor();
        logger.info("Job finished with exit status : " + exitCode);
        return exitCode;
    }
}
