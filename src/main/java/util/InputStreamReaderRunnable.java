package util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class InputStreamReaderRunnable implements Runnable {

    private static final Logger logger = Logger.getLogger(InputStreamReaderRunnable.class);

    private String name = null;
    private BufferedReader bufferedReader = null;

    public InputStreamReaderRunnable(InputStream inputStream, String name) {
        this.name = name;
        this.bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        logger.info("Input Stream reader started for : " + name);
    }

    public void run(){
        try {
            logger.info("Input Stream : " + name);
            String line = bufferedReader.readLine();
            while (line != null){
                logger.info(line);
                line = bufferedReader.readLine();
            }
        }catch (Exception e){
            logger.error("run() failed for name : " + name, e);
        }finally {
            InputOutputUtil.closeBufferReader(bufferedReader);
        }
    }
}
