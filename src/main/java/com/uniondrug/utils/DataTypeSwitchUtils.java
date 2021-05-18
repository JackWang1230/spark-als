package com.uniondrug.utils;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据类型转换类
 * @author RWang
 * @Date 2020/12/17
 */

public class DataTypeSwitchUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DataTypeSwitchUtils.class);

    /**
     * 获取对应的数据类型
     * @param type 传入数据类型
     * @return
     */
    public static DataType getDataType(String type){

        if ("string".equals(type)){
            return DataTypes.StringType;
        }else if ("double".equals(type)){
            return DataTypes.DoubleType;
        }else if ("int".equals(type)){
            return DataTypes.IntegerType;
        }else if ("long".equals(type)){
            return DataTypes.LongType;
        }else if ("short".equals(type)){
            return DataTypes.ShortType;
        }else if ("date".equals(type)){
            return DataTypes.DateType;
        }else if ("boolean".equals(type)){
            return DataTypes.BooleanType;
        }else if ("binary".equals(type)){
            return DataTypes.BinaryType;
        }else if ("timestamp".equals(type)){
            return DataTypes.TimestampType;
        }else if ("calendar".equals(type)){
            return DataTypes.CalendarIntervalType;
        }else if ("null".equals(type) | JavaStringUtils.isEmpty(type)){
            return DataTypes.NullType;
        }
        return null;

    }
}
