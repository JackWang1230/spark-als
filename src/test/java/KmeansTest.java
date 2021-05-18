import com.uniondrug.config.ConfigProperties;
import com.uniondrug.constants.ConfigConstants;
import com.uniondrug.transfomation.AlsDataETL;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @author RWang
 * @Date 2021/4/6
 */

public class KmeansTest {

    private static Properties properties=null;
    public static void main(String[] args) {

        // properties = ConfigProperties.getConfigProperties();
        ConfigProperties configProperties = new ConfigProperties();
        properties = configProperties.getConfigPropertiesTest();
        SparkSession spark = ConfigProperties.initSpark(properties);
        Dataset<Row> hotGoods = spark.read().format(properties.getProperty(ConfigConstants.HIVE_FILE_FORMAT_AND_TABLE_NAME).split(ConfigConstants.PIPE_SEP)[0])
                .table(properties.getProperty(ConfigConstants.HIVE_FILE_FORMAT_AND_TABLE_NAME).split(ConfigConstants.PIPE_SEP)[2]);
        hotGoods.show();
        hotGoods.write().format("csv").save("/Users/wangrui/Documents/spark-als/src/main/resources/ee");
        //        Dataset<Row> recommendResult = AlsDataETL.getRecommendResultV1(properties);
//        recommendResult.show();

    }
}
