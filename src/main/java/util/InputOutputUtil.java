package util;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;

public class InputOutputUtil {
    private static Logger logger = Logger.getLogger(InputOutputUtil.class);

    public static void closeBufferReader(BufferedReader reader) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        }
        catch (Exception ignore) {
            logger.error("Exception caught while closing the stream.", ignore);
        }
    }

    public static void closeInputStream(InputStream reader) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        }
        catch (Exception ignore) {
            logger.error("Exception caught while closing the stream.", ignore);
        }
    }

    public static void closeFSDataOutputStream(FSDataOutputStream fsDataOutputStream) {
        if (fsDataOutputStream == null) {
            return;
        }
        try {
            fsDataOutputStream.close();
        }
        catch (Exception ignore) {
            logger.error("Exception caught while closing the FS Data Output Stream.", ignore);
        }
    }
}
