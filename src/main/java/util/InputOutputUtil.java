package util;

import java.io.BufferedReader;

public class InputOutputUtil {
    public static void close(BufferedReader reader) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        }
        catch (Exception ignore) {
        }
    }
}
