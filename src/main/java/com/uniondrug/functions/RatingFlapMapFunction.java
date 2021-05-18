package com.uniondrug.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple1;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 评分rating进行扁平化处理
 * @author RWang
 * @Date 2020/12/22
 */

public class RatingFlapMapFunction implements FlatMapFunction<Tuple1<Rating[]>, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(RatingFlapMapFunction.class);

    final ArrayList<Row> mapList = new ArrayList<>();

    @Override
    public Iterator<Row> call(Tuple1<Rating[]> tuple1) throws Exception {

        for (Rating rating : tuple1._1) {
            mapList.add(RowFactory.create(rating.user(), rating.product(), rating.rating()));
        }
        return mapList.iterator();
    }

}
