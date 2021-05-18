package com.uniondrug.functions;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * 将Rating(userId,itemId,rating)->Tuple2((userId,itemId),rating)
 * @author RWang
 * @Date 2020/12/28
 */

public class RatingToTuple2PairFunction implements PairFunction<Rating, Tuple2<Integer, Integer>, Double> {

    private static final Logger LOG = LoggerFactory.getLogger(RatingToTuple2PairFunction.class);

    @Override
    public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {

        return Tuple2.apply(Tuple2.apply(rating.user(), rating.product()), rating.rating());
    }
}
