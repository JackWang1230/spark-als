package com.uniondrug.constants;

/**
 * 配置类常量
 * @author RWang
 * @Date 2020/12/17
 */

public class ConfigConstants {

    /**
     * 配置文件所在路径
     */
    public static final String CONFIG_PATH = "/spark-config-rc.properties";

    /**
     * 配置文件所在路径
     */
    public static final String CONFIG_PATH_LOCAL = "/spark-config-local.properties";
    /**
     * 数据所在文件路径
     */
    public static final String DATA_PATH = "src/main/resources/new_data.txt";

    /**
     * spark任务名称
     */
    public static final String SPARK_APPNAME = "spark.appName";
    /**
     * spark配置核数
     */
    public static final String SPARK_MASTER = "spark.master";
    /**
     * redis的host地址
     */
    public static final String REDIS_HOST= "spark.redis.host.kv";
    /**
     * redis的port端口号
     */
    public static final String REDIS_PORT= "spark.redis.port.kv";
    /**
     * redis的密码
     */
    public static final String REDIS_AUTH= "spark.redis.auth.kv";
    /**
     * redis的database
     */
    public static final String REDIS_DB= "spark.redis.db.kv";
    /**
     * spark连接hive的数据源地址
     */
    public static final String HIVE_META_ADDR= "spark.hive.metastore.connect";
    /**
     * hive数据的文件格式以及表名称
     */
    public static final String HIVE_FILE_FORMAT_AND_TABLE_NAME= "spark.hive.file.format.tableName";
    /**
     * spark读取评分矩阵的表结构
     */
    public static final String SPARK_FIELDS_RATING_COLUMNS = "spark.fields.rating.columns";

    /**
     * spark读取评分矩阵的表结构
     */
    public static final String SPARK_FIELDS_RATING_MAPPING_COLUMNS = "spark.fields.rating.mapping.columns";
    /**
     * spark读取用户维度映射的表结构
     */
    public static final String SPARK_FIELDS_USER_COLUMNS = "spark.fields.user.columns";

    /**
     * spark读取物品维度的映射表结构
     */
    public static final String SPARK_FIELDS_ITEM_COLUMNS = "spark.fields.item.columns";
    /**
     * spark读取推荐结果表结构
     */
    public static final String SPARK_FIELDS_RECOMMEND_RESULT_COLUMNS = "spark.fields.recommend.result.columns";
    /**
     * als入参维度的权重值
     */
    public static final String ALS_DATA_ARGS_WEIGHT ="als.data.args.weight";
    /**
     * als算法矩阵秩参数
     */
    public static final String ALS_RANK = "als.model.rank";
    /**
     * als算法迭代次数
     */
    public static final String ALS_ITER = "als.model.iterations";
    /**
     * als算法正则化参数
     */
    public static final String ALS_LAMBDA = "als.model.lambda";
    /**
     * als给用户推荐的商品数
     */
    public static final String ALS_RECOMMEND_NUM = "als.model.recommend.num";
    /**
     *als推荐算法结果表名
     */
    public static final String ALS_RECOMMEND_TABLE = "als.model.recommend.table.name";
    /**
     * spark-redis源码类路径
     */
    public static final String SPARK_REDIS_ORIGIN_CODE_ROUTE = "spark.redis.origin.code.route";
    /**
     * spark-redis源码表名
     */
    public static final String SPARK_REDIS_ORIGIN_CODE_TABLE = "spark.redis.origin.code.table";
    /**
     * spark-redis源码表字段
     */
    public static final String SPARK_REDIS_ORIGIN_CODE_COLS = "spark.redis.origin.code.column";
    /**
     * spark-redis源码表pattern
     */
    public static final String SPARK_REDIS_ORIGIN_CODE_PATTERN = "spark.redis.origin.code.pattern";
    /**
     * spark-redis源码失效时间(秒级别)
     */
    public static final String SPARK_REDIS_ORIGIN_CODE_EXPIRE = "spark.redis.origin.code.expire";
    /**
     * 热门商品的表名
     */
    public static final String SPARK_REDIS_HOT_GOODS_TABLE = "spark.redis.hot.goods.table";
    /**
     * 热门商品的表字段
     */
    public static final String SPARK_REDIS_HOT_GOODS_COLUMN = "spark.redis.hot.goods.column";
    /**
     * 竖线分隔符
     */
    public static final String PIPE_SEP = "\\|";
    /**
     * 冒号分隔符
     */
    public static final String COLON_SEP_TRANS = "\\:";
    /**
     * 逗号分隔符
     */
    public static final String COMMA_SEP_TRANS = "\\,";
}
