package com.uniondrug.utils;

import com.uniondrug.constants.ConfigConstants;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Objects;

/**
 * 表结构自动生成类
 * @author RWang
 * @Date 2020/12/17
 */

public class SchemaFieldsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaFieldsUtils.class);

    /**
     * 获取数据表结构dataSet
     * @param sqlContext sqlContext
     * @param map rdd数据
     * @param fields 字段名称
     * @return dataset
     */
    public static Dataset<Row> getSchemaStructDF(SQLContext sqlContext, JavaRDD<Row> map, String fields){
        StructType structType = getSchemaStruct(fields);
        return sqlContext.createDataFrame(map, structType).toDF();
    }


    /**
     * 获取数据表结构
     * @param fields 字段名称
     * @return StructType
     */
    public static StructType getSchemaStruct(String fields){
        ArrayList<StructField> structFields = new ArrayList<>();
        String[] maps = fields.split(ConfigConstants.PIPE_SEP);
        for (String value:maps) {
            String[] kv = value.split(ConfigConstants.COLON_SEP_TRANS);
            structFields.add(DataTypes.createStructField(kv[0],
                    Objects.requireNonNull(DataTypeSwitchUtils.getDataType(kv[1])),true));
        }
        return DataTypes.createStructType(structFields);

    }
}
