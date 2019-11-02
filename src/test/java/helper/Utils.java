package helper;

import org.apache.commons.io.IOUtils;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Utils {
    public static String classPathResourceContent(String resourceName) {
        try {
            return IOUtils.toString(Utils.class.getResource(resourceName), UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
