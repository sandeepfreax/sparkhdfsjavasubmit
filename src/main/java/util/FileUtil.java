package util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileUtil {
    private static Logger logger = Logger.getLogger(FileUtil.class);

    public static Map<String, String> getMapFromConfigFile(BufferedReader bufferedReader) throws IOException {
        Map<String, String> inputMap = new HashMap<String, String>();
        String line = bufferedReader.readLine();
        logger.info("Started reading the conf file");
        while (line != null){
            logger.info("File Content : " + line);
            String[] keyValue = line.split("=");
            inputMap.put(keyValue[0].trim(), keyValue[1].trim());
            line = bufferedReader.readLine();
        }
        return inputMap;
    }
}
