package sql;

/**
 * Created by lijingxiao on 2018/10/10.
 */
public class Boy {
    private long id;
    private String name;
    private int age;
    private double fv;

    public Boy(double fv, int age, long id, String name) {
        this.fv = fv;
        this.age = age;
        this.id = id;
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public double getFv() {
        return fv;
    }

    public void setFv(double fv) {
        this.fv = fv;
    }
}
