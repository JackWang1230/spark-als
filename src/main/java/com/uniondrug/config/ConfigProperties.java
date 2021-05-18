package com.uniondrug.config;

import com.uniondrug.constants.ConfigConstants;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置类获取
 * @author RWang
 * @Date 2020/12/17
 */

public class ConfigProperties {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigProperties.class);
    private static SparkSession spark = null;

    /**
     * 加载配置文件
     * @return Properties
     */
    public Properties getConfigProperties(){
        try {
            Properties properties = new Properties();
            InputStream in = this.getClass().getResourceAsStream(ConfigConstants.CONFIG_PATH);
            properties.load(new BufferedInputStream(in));
            return properties;
        } catch (Exception e) {
            LOG.error("read spark config error");
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 加载配置文件
     * @return Properties
     */
    public Properties getConfigPropertiesTest(){
        try {
            Properties properties = new Properties();
            InputStream in = this.getClass().getResourceAsStream(ConfigConstants.CONFIG_PATH_LOCAL);
            properties.load(new BufferedInputStream(in));
            return properties;
        } catch (Exception e) {
            LOG.error("read spark config error");
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取spark的session会话
     * @return SparkSession
     */
    public static SparkSession initSpark(Properties prop) {
        if (spark == null) {
            try {
                spark = SparkSession
                        .builder()
                        .appName(prop.getProperty(ConfigConstants.SPARK_APPNAME))
                        .config(prop.getProperty(ConfigConstants.REDIS_HOST).split(ConfigConstants.PIPE_SEP)[0],
                                prop.getProperty(ConfigConstants.REDIS_HOST).split(ConfigConstants.PIPE_SEP)[1])
                        .config(prop.getProperty(ConfigConstants.REDIS_PORT).split(ConfigConstants.PIPE_SEP)[0],
                                Integer.parseInt(prop.getProperty(ConfigConstants.REDIS_PORT).split(ConfigConstants.PIPE_SEP)[1]))
                        .config(prop.getProperty(ConfigConstants.REDIS_AUTH).split(ConfigConstants.PIPE_SEP)[0],
                                prop.getProperty(ConfigConstants.REDIS_AUTH).split(ConfigConstants.PIPE_SEP)[1])
                        .config(prop.getProperty(ConfigConstants.REDIS_DB).split(ConfigConstants.PIPE_SEP)[0],
                                prop.getProperty(ConfigConstants.REDIS_DB).split(ConfigConstants.PIPE_SEP)[1])
                        .config(prop.getProperty(ConfigConstants.HIVE_META_ADDR).split(ConfigConstants.PIPE_SEP)[0],
                                prop.getProperty(ConfigConstants.HIVE_META_ADDR).split(ConfigConstants.PIPE_SEP)[1])
                         .enableHiveSupport()
                        .master(prop.getProperty(ConfigConstants.SPARK_MASTER))
                        .getOrCreate();
                LOG.info("spark session achieve success");
            } catch (Exception e){
                e.printStackTrace();
                LOG.error("fail to get spark session");
            }
        }
        return spark;
    }

    /**
     * 获取JavaSparkContext会话
     * @return JavaSparkContext
     */
    public static JavaSparkContext getJavaSparkContext(Properties prop){
        return  new JavaSparkContext(initSpark(prop).sparkContext());
    }

    /**
     * 获取SQLContext会话
     * @return SQLContext
     */
    public static SQLContext SQLContext(Properties prop){
        JavaSparkContext jsc = new JavaSparkContext(initSpark(prop).sparkContext());
        return  new SQLContext(jsc);
    }

    public static SQLContext SQLContext(SparkSession spark){
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        return  new SQLContext(jsc);
    }
}
