package com.uniondrug.transfomation;

import com.uniondrug.config.ConfigProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * als算法主类
 * @author RWang
 * @Date 2020/12/17
 */

public class AlsRecommend {

    private static final Logger log= LoggerFactory.getLogger(AlsRecommend.class);
    private static  Properties properties=null;

    /**
     * 主入口 als推荐算法结果存储到redis
     * @param args 默认无参
     */
    public static void main(String[] args) {

        // properties = ConfigProperties.getConfigProperties();
        ConfigProperties configProperties = new ConfigProperties();
        properties = configProperties.getConfigProperties();
        Dataset<Row> recommendResult = AlsDataETL.getRecommendResult(properties);
        AlsDataETL.writeRecommendResult2Redis(recommendResult,properties);
    }
}
