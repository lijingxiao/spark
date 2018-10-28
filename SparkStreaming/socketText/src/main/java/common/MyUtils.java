package common;

import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by lijingxiao on 2018/10/28.
 */
public class MyUtils {
    public static long ip2Long(String ip) {
        String[] fragments = ip.split("[.]");
        long ipNum = 0;
        for (String i: fragments) {
            long num = ipNum <<8;
            ipNum = Long.valueOf(i) | num;
        }
        return ipNum;
    }

    public static List<Tuple3<Long, Long, String>> readFile() {
        List<Tuple3<Long, Long, String>> rules = new ArrayList<>();
        URL url = MyUtils.class.getClassLoader().getResource("ip.txt");
        String file = url.getFile();
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = bufferedReader.readLine();
            while (line != null) {
                String[] split = line.split("[|]");
                rules.add(new Tuple3<>(Long.valueOf(split[2]), Long.valueOf(split[3]), split[6]));
                line = bufferedReader.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rules;
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

    public static void data2Mysql(Iterator<Tuple2<String, Integer>> it) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&password=123456&useUnicode=true&characterEncoding=UTF8&useSSL=false");
            PreparedStatement pstm = connection.prepareStatement("insert into access_log values (?, ?)");
            while (it.hasNext()) {
                Tuple2<String, Integer> tp = it.next();
                pstm.setString(1, tp._1);
                pstm.setInt(2, tp._2);
                pstm.executeUpdate();
            }
            if (pstm != null) {
                pstm.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String ip = "112.113.114.67";
        long ipNum = ip2Long(ip);

        List<Tuple3<Long, Long, String>> ipinfos = readFile();
//        for (int i=0; i<10; i++) {
//            System.out.println(ipinfos.get(i).getIpStart() + " " + ipinfos.get(i).getIpEnd() + " " + ipinfos.get(i).getProvince());
//        }

        System.out.println(ipNum);
        int index = binarySearch(ipinfos, ipNum);
        System.out.println(ipinfos.get(index)._1() + " " +  ipinfos.get(index)._2() + " " +  ipinfos.get(index)._3());
    }
}
