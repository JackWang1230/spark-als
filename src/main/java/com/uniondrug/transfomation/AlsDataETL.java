package com.uniondrug.transfomation;

import com.uniondrug.config.ConfigProperties;
import com.uniondrug.constants.ConfigConstants;
import com.uniondrug.constants.SqlConstants;
import com.uniondrug.functions.HiveDataTransFunction;
import com.uniondrug.functions.OriginNameFunction;
import com.uniondrug.functions.RatingFlapMapFunction;
import com.uniondrug.functions.RatingFunction;
import com.uniondrug.model.AlgoModEval;
import com.uniondrug.model.AlgorithmModel;
import com.uniondrug.utils.SchemaFieldsUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;


/**
 * als算法数据处理
 * @author RWang
 * @Date 2020/12/18
 */

public class AlsDataETL {

    private static final Logger LOG = LoggerFactory.getLogger(AlsDataETL.class);

    private static SparkSession spark=null;
    private static SQLContext sqlContext=null;
    private static Dataset<Row> mapUser=null;
    private static Dataset<Row> mapItem=null;
    private static String userField=null;
    private static String itemField=null;

    /**
     * 将入参数据转换成所需的评分dataset
     * @param outPath 输入的数据源数据
     * @return 返回值为评分数据
     */
    public static  Dataset<Row> dealData(Properties prop, String outPath){

        JavaRDD<String> stringJavaRDD = ConfigProperties.getJavaSparkContext(prop).textFile(outPath);
        sqlContext = ConfigProperties.SQLContext(prop);
        JavaRDD<Row> map = stringJavaRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] s = line.split(",");
                return RowFactory.create(s[0], s[1], (double) ((Integer.parseInt(s[2]) +Integer.parseInt(s[3])*2 + 3*Integer.parseInt(s[4]))));
                //return RowFactory.create(s[0], s[1], 3*Double.parseDouble(s[2])+2*Double.parseDouble(s[3]));
            }
        });
        Dataset<Row> hotGoods = sqlContext.read().format(prop.getProperty(ConfigConstants.HIVE_FILE_FORMAT_AND_TABLE_NAME).split(ConfigConstants.PIPE_SEP)[0])
                .table(prop.getProperty(ConfigConstants.HIVE_FILE_FORMAT_AND_TABLE_NAME).split(ConfigConstants.PIPE_SEP)[2]);
        writeHotGoodsResult2Redis(hotGoods,prop);
        return SchemaFieldsUtils.getSchemaStructDF(sqlContext, map, prop.getProperty(ConfigConstants.SPARK_FIELDS_RATING_COLUMNS));
    }


    /**
     * 从hive数据仓库获取原始数据
     * @param prop 配置文件属性
     * @return 返回值为评分数据
     */
    public static Dataset<Row> getHiveSource(Properties prop){
        spark = ConfigProperties.initSpark(prop);
        sqlContext = ConfigProperties.SQLContext(spark);
        JavaRDD<Row> rowData = spark.read().format(prop.getProperty(ConfigConstants.HIVE_FILE_FORMAT_AND_TABLE_NAME).split(ConfigConstants.PIPE_SEP)[0])
                .table(prop.getProperty(ConfigConstants.HIVE_FILE_FORMAT_AND_TABLE_NAME).split(ConfigConstants.PIPE_SEP)[1]).toJavaRDD();
        Dataset<Row> hotGoods = spark.read().format(prop.getProperty(ConfigConstants.HIVE_FILE_FORMAT_AND_TABLE_NAME).split(ConfigConstants.PIPE_SEP)[0])
                .table(prop.getProperty(ConfigConstants.HIVE_FILE_FORMAT_AND_TABLE_NAME).split(ConfigConstants.PIPE_SEP)[2]);
        writeHotGoodsResult2Redis(hotGoods,prop);

        JavaRDD<Row> map = rowData.map(new HiveDataTransFunction());

        return SchemaFieldsUtils.getSchemaStructDF(sqlContext, map, prop.getProperty(ConfigConstants.SPARK_FIELDS_RATING_COLUMNS));
    }
    /**
     *  用户对用户及商品进行映射
     * @param originRating 评分dataset
     * @param originField 配置需要进行映射转换的字段
     * @return Dataset<Row>
     */
    public static Dataset<Row> mappingOriginId(Dataset<Row> originRating,String originField){

        String fieldName = originField.split(ConfigConstants.PIPE_SEP)[0].split(ConfigConstants.COLON_SEP_TRANS)[0];
        JavaRDD<Tuple2<Row, Object>> originName = originRating.select(fieldName).distinct().rdd().zipWithIndex().toJavaRDD();
        JavaRDD<Row> originMap = originName.map(new OriginNameFunction());
        return SchemaFieldsUtils.getSchemaStructDF(sqlContext,originMap,originField);

    }

    /**
     * 准备入als模型数据，下一步进入模型训练
     * @param properties 配置文件属性
     * @return RDD<Rating>
     */
    public static RDD<Rating> mappingBackOriginData(Properties properties){

        // Dataset<Row> orgUserItem1 = dealData(properties, ConfigConstants.DATA_PATH).persist();
        Dataset<Row> orgUserItem = getHiveSource(properties).persist();
        // 177489-30 177613-50 177072-20
        // Dataset<Row> orgUserItem = orgUserItem1.filter("rating<20");
        userField = properties.getProperty(ConfigConstants.SPARK_FIELDS_USER_COLUMNS);
        itemField = properties.getProperty(ConfigConstants.SPARK_FIELDS_ITEM_COLUMNS);
        mapUser = AlsDataETL.mappingOriginId(orgUserItem, userField).persist();
        mapItem = AlsDataETL.mappingOriginId(orgUserItem,itemField).persist();

        Dataset<Row> fullScore = orgUserItem
                .join(mapUser, userField.split(ConfigConstants.PIPE_SEP)[0].split(ConfigConstants.COLON_SEP_TRANS)[0])
                .join(mapItem, itemField.split(ConfigConstants.PIPE_SEP)[0].split(ConfigConstants.COLON_SEP_TRANS)[0]);
        // fullScore.show();
        orgUserItem.unpersist();
        JavaRDD<Row> rating = fullScore.select(userField.split(ConfigConstants.PIPE_SEP)[1].split(ConfigConstants.COLON_SEP_TRANS)[0],
                itemField.split(ConfigConstants.PIPE_SEP)[1].split(ConfigConstants.COLON_SEP_TRANS)[0],
                properties.getProperty(ConfigConstants.SPARK_FIELDS_RATING_COLUMNS).split(ConfigConstants.PIPE_SEP)[2].split(ConfigConstants.COLON_SEP_TRANS)[0])
                .rdd().toJavaRDD();
        return rating.map((Function<Row, Rating>) row -> new Rating(row.getInt(0), row.getInt(1), row.getDouble(2))).rdd();
    }

    /**
     * 获取最终的推荐结果
     * @param properties 配置文件属性
     */
    public static Dataset<Row>  getRecommendResult(Properties properties){

        RDD<Rating> ratingRDD = mappingBackOriginData(properties);
        MatrixFactorizationModel model = AlgorithmModel.alsAlgoImplicit(ratingRDD,
                Integer.parseInt(properties.getProperty(ConfigConstants.ALS_RANK)),
                Integer.parseInt(properties.getProperty(ConfigConstants.ALS_ITER)),
                Double.parseDouble(properties.getProperty(ConfigConstants.ALS_LAMBDA)));
        AlgoModEval.getModEvalValue(model, ratingRDD);
        // System.out.println(modEvalValue);
        JavaRDD<Row> mappingMap = model.
                recommendProductsForUsers(Integer.parseInt(properties.getProperty(ConfigConstants.ALS_RECOMMEND_NUM)))
                .toJavaRDD().map(new RatingFunction()).flatMap(new RatingFlapMapFunction()).rdd().toJavaRDD();
        String props = properties.getProperty(ConfigConstants.SPARK_FIELDS_RATING_MAPPING_COLUMNS);
        String originCols = properties.getProperty(ConfigConstants.SPARK_FIELDS_RATING_COLUMNS);
        Dataset<Row> mapValue = SchemaFieldsUtils.getSchemaStructDF(sqlContext, mappingMap, props);
        // mapValue.show();
        Dataset<Row> result = mapValue.join(mapUser, userField.split(ConfigConstants.PIPE_SEP)[1].split(ConfigConstants.COLON_SEP_TRANS)[0])
                .join(mapItem, itemField.split(ConfigConstants.PIPE_SEP)[1].split(ConfigConstants.COLON_SEP_TRANS)[0])
                .select(originCols.split(ConfigConstants.PIPE_SEP)[0].split(ConfigConstants.COLON_SEP_TRANS)[0],
                        originCols.split(ConfigConstants.PIPE_SEP)[1].split(ConfigConstants.COLON_SEP_TRANS)[0],
                        originCols.split(ConfigConstants.PIPE_SEP)[2].split(ConfigConstants.COLON_SEP_TRANS)[0]);
        mapUser.unpersist();
        mapItem.unpersist();
        sqlContext.registerDataFrameAsTable(result,properties.getProperty(ConfigConstants.ALS_RECOMMEND_TABLE));
        return sqlContext.sql(SqlConstants.RECOMMEND_RESULT_TABLE);

    }

    /**
     * 获取优化算法后最终的推荐结果
     * @param properties 配置文件属性
     */
//    public static Dataset<Row>  getRecommendResultV1(Properties properties){
//
//        RDD<Rating> ratingRDD = mappingBackOriginData(properties);
//        MatrixFactorizationModel model = AlgorithmModel.alsAlgo(ratingRDD,
//                Integer.parseInt(properties.getProperty(ConfigConstants.ALS_RANK)),
//                Integer.parseInt(properties.getProperty(ConfigConstants.ALS_ITER)),
//                Double.parseDouble(properties.getProperty(ConfigConstants.ALS_LAMBDA)));
//        AlgoModEval.getModEvalValue(model, ratingRDD);
//
//        JavaRDD<Tuple2<Object, double[]>> tuple2JavaRDD1 = model.userFeatures().toJavaRDD();
//        model.userFeatures().
//
//        model.userFeatures().toJavaRDD().map(new Function<Tuple2<Object, double[]>, Vector>() {
//            @Override
//            public Vector call(Tuple2<Object, double[]> v1) throws Exception {
//                return Vectors.dense(v1._2);
//            }
//        }).rdd();
//        KMeansModel train1 = KMeans.train(rdd, 2, 10);
//
//        JavaRDD<Tuple2<Object, Object>> tuple2JavaRDD = train1.predict(rdd).zipWithIndex().toJavaRDD();
//
//        tuple2JavaRDD.map(new Function<Tuple2<Object, Object>, Row>() {
//            @Override
//            public Row call(Tuple2<Object, Object> v1) throws Exception {
//                return RowFactory.create(v1._1,v1._2);
//            }
//        });
////        objectJavaRDD.foreach(new VoidFunction<Object>() {
////            @Override
////            public void call(Object o) throws Exception {
////                System.out.println(o.toString());
////            }
////        });
//
//
////        JavaRDD<Row> map = model.userFeatures().toJavaRDD().map(new Function<Tuple2<Object, double[]>, Row>() {
////            @Override
////            public Row call(Tuple2<Object, double[]> v1) throws Exception {
////                // RowFactory.create(value._1.get(0), Integer.parseInt(value._2.toString()));
////                return RowFactory.create(Integer.parseInt(v1._1.toString()), Vectors.dense(v1._2));
////            }
////        });
////
////        ArrayList<StructField> structFields = new ArrayList<>();
////
////
////        StructField id = DataTypes.createStructField("id", DataTypes.IntegerType, true);
////        StructField fes = DataTypes.createStructField("fes", DataType.NullType);
////        structFields.add(id);
////        structFields.add(fes);
////        StructType structType = DataTypes.createStructType(structFields);
////
////        Dataset<Row> rowDataset = sqlContext.createDataFrame(map, structType).toDF().orderBy("id");
////        rowDataset.show();
////
////        KMeans fes1 = new KMeans().setFeaturesCol("fes").setK(8).setMaxIter(10);
////        KMeansModel kmsModel = fes1.fit(rowDataset);
//
//
//
//        // System.out.println(modEvalValue);
//        JavaRDD<Row> mappingMap = model.
//                recommendProductsForUsers(Integer.parseInt(properties.getProperty(ConfigConstants.ALS_RECOMMEND_NUM)))
//                .toJavaRDD().map(new RatingFunction()).flatMap(new RatingFlapMapFunction()).rdd().toJavaRDD();
//        String props = properties.getProperty(ConfigConstants.SPARK_FIELDS_RATING_MAPPING_COLUMNS);
//        String originCols = properties.getProperty(ConfigConstants.SPARK_FIELDS_RATING_COLUMNS);
//        Dataset<Row> mapValue = SchemaFieldsUtils.getSchemaStructDF(sqlContext, mappingMap, props);
//        mapValue.show();
//        Dataset<Row> result = mapValue.join(mapUser, userField.split(ConfigConstants.PIPE_SEP)[1].split(ConfigConstants.COLON_SEP_TRANS)[0])
//                .join(mapItem, itemField.split(ConfigConstants.PIPE_SEP)[1].split(ConfigConstants.COLON_SEP_TRANS)[0])
//                .select(originCols.split(ConfigConstants.PIPE_SEP)[0].split(ConfigConstants.COLON_SEP_TRANS)[0],
//                        originCols.split(ConfigConstants.PIPE_SEP)[1].split(ConfigConstants.COLON_SEP_TRANS)[0],
//                        originCols.split(ConfigConstants.PIPE_SEP)[2].split(ConfigConstants.COLON_SEP_TRANS)[0]);
//        result.show();
//        mapUser.unpersist();
//        mapItem.unpersist();
//        sqlContext.registerDataFrameAsTable(result,properties.getProperty(ConfigConstants.ALS_RECOMMEND_TABLE));
//        return sqlContext.sql(SqlConstants.RECOMMEND_RESULT_TABLE);
//
//    }


    /**
     * 将推荐结果写入redis中
     * @param recommendResult 推荐结果dataset
     * @param properties 配置属性
     */
    public static void writeRecommendResult2Redis(Dataset<Row> recommendResult,Properties properties){

        try {
            // recommendResult.show();
            recommendResult.write()
                    .format(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_ROUTE))
                    .option(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_TABLE).split(ConfigConstants.PIPE_SEP)[0],
                            properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_TABLE).split(ConfigConstants.PIPE_SEP)[1])
                    .option(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_COLS).split(ConfigConstants.PIPE_SEP)[0],
                            properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_COLS).split(ConfigConstants.PIPE_SEP)[1])
                    .option(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_EXPIRE).split(ConfigConstants.PIPE_SEP)[0],
                            Integer.parseInt(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_EXPIRE).split(ConfigConstants.PIPE_SEP)[1]))
                    .mode(SaveMode.Overwrite)
                    .save();
            LOG.info("success write recommend result to redis");
            // readRecommendResultFromRedis(properties);

        }catch (Exception e){
            LOG.error("write recommend result to  redis error");
        }finally {
            spark.close();
        }
    }

    /**
     * 利用spark从redis中获取将推荐结果
     * @param properties 配置属性
     * @return Dataset<Row>
     */
    public static Dataset<Row> readRecommendResultFromRedis(Properties properties){

        // 获取表结构
        StructType recommendSchemaStruct = SchemaFieldsUtils.getSchemaStruct(properties.getProperty(ConfigConstants.SPARK_FIELDS_RECOMMEND_RESULT_COLUMNS));
        Dataset<Row> load = sqlContext.read()
                .format(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_ROUTE))
                .schema(recommendSchemaStruct)
                .option(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_PATTERN).split(ConfigConstants.PIPE_SEP)[0],
                        properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_PATTERN).split(ConfigConstants.PIPE_SEP)[1])
                .option(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_COLS).split(ConfigConstants.PIPE_SEP)[0],
                        properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_COLS).split(ConfigConstants.PIPE_SEP)[1])
                .load();
        load.show();
        return load;
    }

    /**
     * 将销量热门商品存储到redis
     * @param hotGoodsResult 热门商品数据（非药和O2O）
     * @param properties 配置属性
     */
    public static void writeHotGoodsResult2Redis(Dataset<Row> hotGoodsResult,Properties properties){

        try {
            // hotGoodsResult.show();
            hotGoodsResult.write()
                    .format(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_ROUTE))
                    .option(properties.getProperty(ConfigConstants.SPARK_REDIS_HOT_GOODS_TABLE).split(ConfigConstants.PIPE_SEP)[0],
                            properties.getProperty(ConfigConstants.SPARK_REDIS_HOT_GOODS_TABLE).split(ConfigConstants.PIPE_SEP)[1])
                    .option(properties.getProperty(ConfigConstants.SPARK_REDIS_HOT_GOODS_COLUMN).split(ConfigConstants.PIPE_SEP)[0],
                            properties.getProperty(ConfigConstants.SPARK_REDIS_HOT_GOODS_COLUMN).split(ConfigConstants.PIPE_SEP)[1])
                    .option(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_EXPIRE).split(ConfigConstants.PIPE_SEP)[0],
                            Integer.parseInt(properties.getProperty(ConfigConstants.SPARK_REDIS_ORIGIN_CODE_EXPIRE).split(ConfigConstants.PIPE_SEP)[1]))
                    .mode(SaveMode.Overwrite)
                    .save();
            LOG.info("success write hot goods result to redis");
            // readRecommendResultFromRedis(properties);

        }catch (Exception e){
            LOG.error("write hot goods result to redis error");
        }
    }

}
