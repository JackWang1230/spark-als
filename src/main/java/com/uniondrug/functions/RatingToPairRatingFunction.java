package com.uniondrug.functions;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * 将rating(userId,itemId,rating)->Tuple2(userId,itemId)
 * @author RWang
 * @Date 2020/12/28
 */

public class RatingToPairRatingFunction implements PairFunction<Rating, Integer, Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(RatingToPairRatingFunction.class);

    @Override
    public Tuple2<Integer, Integer> call(Rating rating) throws Exception {

        return Tuple2.apply(rating.user(), rating.product());
    }
}
