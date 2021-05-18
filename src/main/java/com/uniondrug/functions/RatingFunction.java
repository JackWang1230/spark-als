package com.uniondrug.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple1;
import scala.Tuple2;

/**
 * rating进行提取值
 * @author RWang
 * @Date 2020/12/22
 */

public class RatingFunction implements Function<Tuple2<Object, Rating[]>, Tuple1<Rating[]>> {

    private static final Logger LOG = LoggerFactory.getLogger(RatingFunction.class);

    @Override
    public Tuple1<Rating[]> call(Tuple2<Object, Rating[]> v1) throws Exception {
        return Tuple1.apply(v1._2);
    }
}
