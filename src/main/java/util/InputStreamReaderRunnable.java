package util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;

public class InputStreamReaderRunnable implements Runnable {

    private static final Logger logger = Logger.getLogger(InputStreamReaderRunnable.class);

    private String name;
    private BufferedReader bufferedReader;
    private String logPath;
    private FileSystem fileSystem;

    public InputStreamReaderRunnable(InputStream inputStream, String name, String logPath, FileSystem fileSystem) {
        this.name = name;
        this.bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        this.logPath = logPath;
        this.fileSystem = fileSystem;
        logger.info("Input Stream reader started for : " + name);
    }

    public void run(){
        try {
            logger.info("Input Stream : " + name);

            Calendar calendar = Calendar.getInstance();
            String dateFormat = "" + calendar.get(Calendar.YEAR) +
                            calendar.get(Calendar.MONTH + 1) + calendar.get(Calendar.DATE) +
                            calendar.get(Calendar.HOUR) + calendar.get(Calendar.MINUTE) +
                            calendar.get(Calendar.SECOND);
            String filePath = logPath + "/" + name + "_" + dateFormat + ".log";
            logger.info("Log Path : " + filePath);

            StringBuffer stringBuffer = new StringBuffer();
            String line = bufferedReader.readLine();
            while (line != null){
                stringBuffer.append(line.trim());
                line = bufferedReader.readLine();
            }
            FileUtils.writeContentToHdfs(fileSystem, filePath, stringBuffer.toString());
        }catch (Exception e){
            logger.error("run() failed for name : " + name, e);
        }finally {
            InputOutputUtil.closeBufferReader(bufferedReader);
        }
    }
}
