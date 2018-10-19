package sql.IPLocation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Tuple3;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by lijingxiao on 2018/10/12.
 */
public class IpLocation2 {
    public static long ip2Long(String ip) {
        String[] fragments = ip.split("[.]");
        long ipNum = 0;
        for (String i: fragments) {
            long num = ipNum <<8;
            ipNum = Long.valueOf(i) | num;
        }
        return ipNum;
    }
    public static int binarySearch(List<Tuple3<Long, Long, String>> rules, long ip) {
        if (rules == null || rules.size()<=0)
            return -1;
        int low=0, high=rules.size()-1;
        while (low < high ) {
            int middle = (low + high)/2;
            if (ip >= rules.get(middle)._1() && ip<= rules.get(middle)._2()) {
                return middle;
            } else if (ip < rules.get(middle)._1()) {
                high = middle-1;
            } else {
                low = middle+1;
            }
        }
        return -1;
    }
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("ipLocation").master("local[4]").getOrCreate();
        JavaRDD<String> ipRules = spark.read().textFile("ip.txt").toJavaRDD();
        JavaRDD<Tuple3<Long, Long, String>> ruleRDD = ipRules.map(line -> {
            String[] fileds = line.split("[|]");
            //Long.valueOf(split[2]), Long.valueOf(split[3]), split[6]
            Long ipStart = Long.valueOf(fileds[2]);
            Long ipEnd = Long.valueOf(fileds[3]);
            String province = fileds[6];
            return new Tuple3<>(ipStart, ipEnd, province);
        });
        List<Tuple3<Long, Long, String>> rules = ruleRDD.collect();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//        ClassTag<List<Tuple3<Long, Long, String>>> tag = scala.reflect.ClassTag$.MODULE$.apply(Tuple3.class);
        Broadcast<List<Tuple3<Long, Long, String>>> broadcast = jsc.broadcast(rules);

        JavaRDD<String> logRDD = spark.read().textFile("access.log").toJavaRDD();
//        JavaRDD<Row> ipLogRDD = logRDD.map(line -> {
//            String province = "未知";
//            String[] fileds = line.split("[|]");
//            String ip = fileds[1];
//            long ip2Long = ip2Long(ip);
//            List<Tuple3<Long, Long, String>> ruleRef = broadcast.value();
//            int index = binarySearch(ruleRef, ip2Long);
//            if (index != -1) {
//                province = ruleRef.get(index)._3();
////                province = ipinfos.get(index).getProvince();
//            }
//            return RowFactory.create(province);
//        });

        JavaRDD<Row> ipLogRDD = logRDD.map(line -> {
            String[] fileds = line.split("[|]");
            String ip = fileds[1];
            long ip2Long = ip2Long(ip);
            return RowFactory.create(ip2Long);
        });

        //自定义UDF
        spark.udf().register("ip2Province", new UDF1<Long, String>() {
            @Override
            public String call(Long ip) throws Exception {
                //查找ip规则（事先已经广播了，已经在Executor中了）
                //函数的逻辑是在Executor中执行的，怎样获取ip规则的对应的数据呢？
                //使用广播变量的引用，就可以获得
                List<Tuple3<Long, Long, String>> rulesRef = broadcast.value();
                //根据IP地址对应的十进制查找省份名称
                int index = binarySearch(rulesRef, ip);
                String province = "未知";
                if(index != -1) {
                    province = rulesRef.get(index)._3();
                }
                return province;
            }
        }, DataTypes.StringType);

        ArrayList<StructField> structFieldsLog = new ArrayList<>();
        structFieldsLog.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
        StructType schemaLog = DataTypes.createStructType(structFieldsLog);
        Dataset<Row> logDf = spark.createDataFrame(ipLogRDD, schemaLog);
        try {
            logDf.createTempView("v_log");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        Dataset<Row> result = spark.sql("SELECT ip2Province(ip) province, count(*) counts FROM v_log GROUP BY province ORDER BY counts DESC");
//        Dataset<Row> result = spark.sql("SELECT province, count(*) counts FROM v_log GROUP BY province ORDER BY counts DESC");

        result.show();

        spark.stop();
    }
}
