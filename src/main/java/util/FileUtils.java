package util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class FileUtils {
    private static Logger logger = Logger.getLogger(FileUtils.class);

    public static Map<String, String> getMapFromConfigFile(FileSystem fileSystem, String configFileLoc) throws IOException {
        Path configFilePath = new Path(configFileLoc);
        InputStream inputStream = fileSystem.open(configFilePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        Map<String, String> inputMap = new HashMap<String, String>();
        String line = bufferedReader.readLine();

        logger.info("Started reading the conf file");
        while (line != null){
            if(!line.isEmpty() && !line.startsWith("#")){
                String[] keyValue = line.split("=");
                inputMap.put(keyValue[0].trim(), keyValue[1].trim());
            }
            line = bufferedReader.readLine();
        }
        return inputMap;
    }
}
