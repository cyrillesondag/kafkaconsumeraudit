package fr.erdf.distribution.kafka;

import java.io.IOException;
import java.util.Properties;

public class Cfg {

    private final Properties properties = new Properties();

    private static final Cfg INSTANCE = new Cfg();

    private Cfg(){
        try {
            properties.load(Cfg.class.getResourceAsStream("config.properties"));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read property file  /config.properties");
        }
    }

    public static String getString(String key){
        String val = System.getProperty(key);
        if(val == null){
            val = INSTANCE.properties.getProperty(key);
        }
        return val;
    }

    public static String getString(String key, String fallack){
        final String val = getString(key);
        return val != null ? val : fallack;
    }

    public static long getLong(String key, long fallback){
        final String val = getString(key);
        return val != null ? Long.valueOf(val) : fallback;
    }

    public static int getInt(String key, int fallback){
        final String val = getString(key);
        return val != null ? Integer.valueOf(val) : fallback;
    }

    public static boolean getBool(String key, boolean fallback){
        final String val = getString(key);
        return val != null ? Boolean.valueOf(val) : fallback;
    }
}
