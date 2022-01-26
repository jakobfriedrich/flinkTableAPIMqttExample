package spendreport;

public class User {

    public Integer userid;

    public String name;

    // default constructor for DataStream API
    public User() {}

    // fully assigning constructor for Table API
    public User(Integer userid, String name) {
        this.userid = userid;
        this.name = name;
    }
}
