package com.uniondrug.functions;

import com.uniondrug.config.ConfigProperties;
import com.uniondrug.constants.ConfigConstants;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 从hive读取数据转换成需要的入模型数据
 * @author RWang
 * @Date 2020/12/24
 */

public class HiveDataTransFunction implements Function<Row, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(HiveDataTransFunction.class);
    private static Properties prop = new ConfigProperties().getConfigProperties();
    private static int clickWeight=0;
    private static int carWeight=0;
    private static int buyWeight=0;

    /**
     * 入模型设置rating对于不同维度的权重值
     * @param v1 入参原始数据(涉及用户的点击，加购，购买)
     * @return 返回Row
     * @throws Exception
     */
    @Override
    public Row call(Row v1) throws Exception {
        //TODO 将加购，购买行为的作为配置进行调参
        // 读取配置项目

        String fullData = prop.getProperty(ConfigConstants.ALS_DATA_ARGS_WEIGHT);
        String[] weightDataList = fullData.split(ConfigConstants.PIPE_SEP)[1].split(ConfigConstants.COMMA_SEP_TRANS);
        clickWeight = Integer.parseInt(weightDataList[0]);
        carWeight = Integer.parseInt(weightDataList[1]);
        buyWeight = Integer.parseInt(weightDataList[2]);

        return RowFactory.create(v1.get(0), v1.get(1), (double) ((clickWeight*v1.getInt(2)+carWeight*v1.getInt(3) + buyWeight*v1.getInt(4))));
    }
}
