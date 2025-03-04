package ConfigReader;

import java.io.InputStream;
import java.util.Properties;
import java.io.IOException;

public class ConfigReader {

    private Properties properties;

    public ConfigReader() {
        properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new IOException("Unable to find config.properties in the classpath");
            }
            properties.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("Error loading config properties", ex);
        }
    }

    public String get(String key) {
        return properties.getProperty(key);
    }

}
