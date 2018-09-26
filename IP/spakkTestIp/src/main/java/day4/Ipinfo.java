package day4;

import java.io.Serializable;

/**
 * Created by lijingxiao on 2018/9/21.
 */
//public class Ipinfo {
public class Ipinfo implements Serializable{
    public long ipStart;
    public long ipEnd;
    public String province;

    public long getIpStart() {
        return ipStart;
    }

    public void setIpStart(long ipStart) {
        this.ipStart = ipStart;
    }

    public long getIpEnd() {
        return ipEnd;
    }

    public void setIpEnd(long ipEnd) {
        this.ipEnd = ipEnd;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }
}
