package com.uniondrug.functions;

import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * 将Tuple2(rating,predict)->JavaPair (rating,predict)
 * @author RWang
 * @Date 2020/12/28
 */

public class RatingAndPredictValuePairFunction implements PairFunction<Tuple2<Double, Double>, Object, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(RatingAndPredictValuePairFunction.class);

    @Override
    public Tuple2<Object, Object> call(Tuple2<Double, Double> value) throws Exception {
        return Tuple2.apply(value._1, value._2);
    }
}
