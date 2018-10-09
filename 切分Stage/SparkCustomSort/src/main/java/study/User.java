package study;

import org.bouncycastle.crypto.tls.UseSRTPData;
import scala.math.Ordered;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by lijingxiao on 2018/9/26.
 */
public class User implements Serializable, Ordered<User> {
    public String name;
    public int age;
    public int fv;

    public User(String name, int age, int fv) {
        this.name = name;
        this.age = age;
        this.fv = fv;
    }

    @Override
    public int compare(User that) {
        if (this.fv == that.fv) {
            return this.age - that.age;
        } else {
            return that.fv - this.fv;
        }
    }

    @Override
    public int compareTo(User that) {
        if (this.fv == that.fv) {
            return this.age - that.age;
        } else {
            return that.fv - this.fv;
        }
    }

    @Override
    public boolean $less(User that) {
        if (this.fv < that.fv) {
            return true;
        } else if (this.fv == that.fv && this.age > that.age) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean $greater(User that) {
        if (this.fv > that.fv) {
            return true;
        } else if (this.fv == that.fv && this.age < that.age) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean $less$eq(User that) {
        if (this.$less(that)) {
            return true;
        } else if (this.fv == that.fv && this.age == that.age) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean $greater$eq(User that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.fv == that.fv && this.age == that.age) {
            return true;
        } else {
            return false;
        }
    }

    //    @Override
//    public int compare(User o1, User o2) {
//        if (o1.fv == o2.fv) {
//            return o1.fv - o2.fv;
//        } else {
//            return o2.age - o1.age;
//        }
//    }


    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", fv=" + fv +
                '}';
    }
}
