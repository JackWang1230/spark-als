package com.uniondrug.model;

import com.uniondrug.transfomation.AlsDataETL;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 算法模型
 * @author RWang
 * @Date 2020/12/17
 */

public class AlgorithmModel {

    private static final Logger LOG = LoggerFactory.getLogger(AlgorithmModel.class);
    /**
     * 获取als算法显式模型 default
     * @param ratings 评分矩阵
     * @param rank 矩阵秩
     * @param iter 迭代次数
     * @param lambda 正则化参数
     * @return MatrixFactorizationModel
     */
    public static MatrixFactorizationModel alsAlgo(RDD<Rating> ratings,int rank,int iter,Double lambda){
        LOG.info("use als model without implicit and suit rating score like film score");
        return ALS.train(ratings, rank, iter, lambda);
    }

    /**
     * 获取als算法隐式反馈模型
     * @param ratings 评分矩阵
     * @param rank 矩阵秩
     * @param iter 迭代次数
     * @param lambda 正则化参数
     * @return MatrixFactorizationModel
     */
    public static MatrixFactorizationModel alsAlgoImplicit(RDD<Rating> ratings,int rank,int iter,Double lambda){
        LOG.info("use als model with implicit and it suit user view behavior");
        return new ALS().setRank(rank).setLambda(lambda).setIterations(iter).setImplicitPrefs(true).setAlpha(0.01).run(ratings);
    }

    /**
     * 获取tf-idf算法模型
     * @param dataset 数据集
     * @return Dataset<Row>
     */
    public static Dataset<Row> tfidfAlgo(Dataset<Row> dataset) {
        Tokenizer tokenizer = new Tokenizer().setInputCol("segment").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(dataset);
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures");
        Dataset<Row> featurizedData = hashingTF.transform(wordsData);
        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData);
        return  idfModel.transform(featurizedData);
    }

    /**
     * 获取k-means算法模型
     */
    public static void  kMeansAlgo(RDD<Vector> data,int k,int iter){
        KMeansModel kMeansModel = KMeans.train(data, k, iter);
        Vector[] vectors = kMeansModel.clusterCenters();
        kMeansModel.predict(data);
    }

}
