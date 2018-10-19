package sql.IPLocation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

/**
 * Created by lijingxiao on 2018/10/12.
 */
public class IpLocation {
    public static long ip2Long(String ip) {
        String[] fragments = ip.split("[.]");
        long ipNum = 0;
        for (String i: fragments) {
            long num = ipNum <<8;
            ipNum = Long.valueOf(i) | num;
        }
        return ipNum;
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("ipLocation").master("local[4]").getOrCreate();
        JavaRDD<String> ipRules = spark.read().textFile("ip.txt").toJavaRDD();
        JavaRDD<Row> rulesRDD = ipRules.map(line -> {
            String[] fileds = line.split("[|]");
            //Long.valueOf(split[2]), Long.valueOf(split[3]), split[6]
            Long ipStart = Long.valueOf(fileds[2]);
            Long ipEnd = Long.valueOf(fileds[3]);
            String province = fileds[6];
            return RowFactory.create(ipStart, ipEnd, province);
        });
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("ipStart", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("ipEnd", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("province", DataTypes.StringType, true));
        StructType ruleSchema = DataTypes.createStructType(structFields);

        Dataset<Row> ruleDf = spark.createDataFrame(rulesRDD, ruleSchema);
        try {
            ruleDf.createTempView("v_rules");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }


        JavaRDD<String> logRDD = spark.read().textFile("access.log").toJavaRDD();
        JavaRDD<Row> ipLogRDD = logRDD.map(line -> {
            String[] fileds = line.split("[|]");
            String ip = fileds[1];
            long ip2Long = ip2Long(ip);
            return RowFactory.create(ip2Long);
        });
        ArrayList<StructField> structFieldsLog = new ArrayList<>();
        structFieldsLog.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
        StructType schemaLog = DataTypes.createStructType(structFieldsLog);
        Dataset<Row> logDf = spark.createDataFrame(ipLogRDD, schemaLog);
        try {
            logDf.createTempView("v_log");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        Dataset<Row> result = spark.sql("SELECT province, count(*) counts FROM v_log JOIN v_rules ON (ip >= ipStart AND ip <= ipEnd) GROUP BY province ORDER BY counts DESC");

        result.show();

        spark.stop();
    }
}
