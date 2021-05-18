package com.uniondrug.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * 原始的用户商品id进行映射
 * @author RWang
 * @Date 2020/12/22
 */

public class OriginNameFunction implements Function<Tuple2<Row, Object>, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(OriginNameFunction.class);

    @Override
    public Row call(Tuple2<Row, Object> value) throws Exception {
        return RowFactory.create(value._1.get(0), Integer.parseInt(value._2.toString()));
    }
}
