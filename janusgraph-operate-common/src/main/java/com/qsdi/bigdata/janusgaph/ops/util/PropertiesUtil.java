package com.qsdi.bigdata.janusgaph.ops.util;

import io.swagger.models.auth.In;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Description
 *
 * @author lijie0203 2024/3/9 15:49
 */
public class PropertiesUtil {
    public final static Logger logger= LoggerFactory.getLogger(PropertiesUtil.class);
    public static Properties properties;
    static {
        String filename="config.properties";
        properties=new Properties();
        try {
            properties.load(new InputStreamReader(PropertiesUtil.class.getClassLoader().getResourceAsStream(filename)));

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("配置文件读取失败",e);
        }
    }
    public static String getValue(String key){
        String val=properties.getProperty(key.trim());
        if(val==null){
            return null;
        }else{
            return val.trim();
        }
    }
    public static String getValue(String key,String defval){
        String val = getValue(key);
        if(val==null){
            return defval;
        }else{
            return val.trim();
        }
    }


    public static Long getLongValue(String key) {
        String val = getValue(key);
        if(val==null){
            return null;
        }else{
            return Long.parseLong(val);
        }
    }

    public static Long getLongValue(String key,long defval) {
        String val = getValue(key);
        if(val==null){
            return defval;
        }else{
            return Long.parseLong(val);
        }
    }

    public static Integer getIntValue(String key) {
        String val = getValue(key);
        if(val==null){
            return null;
        }else{
            return Integer.parseInt(val);
        }
    }

    public static Integer getIntValue(String key,int defval) {
        String val = getValue(key);
        if(val==null){
            return defval;
        }else{
            return Integer.parseInt(val);
        }
    }




}
