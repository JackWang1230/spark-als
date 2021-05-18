package com.uniondrug.model;

import com.uniondrug.functions.RatingAndPredictValuePairFunction;
import com.uniondrug.functions.RatingToPairRatingFunction;
import com.uniondrug.functions.RatingToTuple2PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.MessageFormat;

/**
 * als模型评估类
 * @author RWang
 * @Date 2020/12/28
 */

public class AlgoModEval {

    private static final Logger LOG = LoggerFactory.getLogger(AlgoModEval.class);
    /**
     * 模型评估算法好坏值
     * @param model 入参模型
     * @param ratingRDD 映射完成后的打分rdd
     * @return 最终的均方差和均方根差值
     */
    public static String getModEvalValue(MatrixFactorizationModel model, RDD<Rating> ratingRDD){

        JavaPairRDD<Integer, Integer> userItemRDD = ratingRDD.toJavaRDD().mapToPair(new RatingToPairRatingFunction());
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = model.predict(userItemRDD).mapToPair(new RatingToTuple2PairFunction());
        JavaPairRDD<Tuple2<Integer, Integer>, Double> ratings = ratingRDD.toJavaRDD().mapToPair(new RatingToTuple2PairFunction());
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> ratingsAndPredictions = ratings.join(predictions);
        RDD<Tuple2<Object, Object>> rdd = ratingsAndPredictions.values().mapToPair(new RatingAndPredictValuePairFunction()).rdd();
        RegressionMetrics regressionMetrics = new RegressionMetrics(rdd);
        double meanSquaredErrorValue = regressionMetrics.meanSquaredError();
        double rootMeanSquaredErrorValue = regressionMetrics.rootMeanSquaredError();
        LOG.warn(MessageFormat.format("Mean Squared Error ={0} and Root Mean Squared Error ={1}",
                meanSquaredErrorValue,rootMeanSquaredErrorValue));
        return MessageFormat.format("Mean Squared Error ={0} and Root Mean Squared Error ={1}",
                meanSquaredErrorValue,rootMeanSquaredErrorValue);
    }
}
