import com.uniondrug.config.ConfigProperties;
import com.uniondrug.constants.ConfigConstants;
import com.uniondrug.transfomation.AlsDataETL;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.MessageFormat;
import java.util.Properties;

/**
 * @author RWang
 * @Date 2020/12/24
 */

public class SparkTest {


    private static SparkSession spark = null;
    public static SparkSession initSpark(Properties prop) {
        if (spark == null) {
            spark = SparkSession
                    .builder()
                    .appName(prop.getProperty(ConfigConstants.SPARK_APPNAME))
                    .config(prop.getProperty(ConfigConstants.REDIS_HOST).split(ConfigConstants.PIPE_SEP)[0],
                            prop.getProperty(ConfigConstants.REDIS_HOST).split(ConfigConstants.PIPE_SEP)[1])
                    .config(prop.getProperty(ConfigConstants.REDIS_PORT).split(ConfigConstants.PIPE_SEP)[0],
                            Integer.parseInt(prop.getProperty(ConfigConstants.REDIS_PORT).split(ConfigConstants.PIPE_SEP)[1]))
                    .config("hive.metastore.uris","thrift://test01:9083")
//                    .config(prop.getProperty(ConfigConstants.REDIS_AUTH).split(ConfigConstants.PIPE_SEP)[0],
//                            prop.getProperty(ConfigConstants.REDIS_AUTH).split(ConfigConstants.PIPE_SEP)[1])
//                    .config(prop.getProperty(ConfigConstants.REDIS_DB).split(ConfigConstants.PIPE_SEP)[0],
//                            prop.getProperty(ConfigConstants.REDIS_DB).split(ConfigConstants.PIPE_SEP)[1])
                    .enableHiveSupport()
                    .master(prop.getProperty(ConfigConstants.SPARK_MASTER))
                    .getOrCreate();
        }
        return spark;
    }

    public static void main(String[] args) {

    }
}
