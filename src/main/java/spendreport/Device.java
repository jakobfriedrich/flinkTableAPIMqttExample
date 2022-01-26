package spendreport;

public class Device {

    public Integer deviceid;

    public String name;

    public String status;

    public String event_time;

    // default constructor for DataStream API
    public Device() {}

    // fully assigning constructor for Table API
    public Device(Integer deviceid, String name, String status, String event_time) {
        this.deviceid = deviceid;
        this.name = name;
        this.status = status;
        this.event_time = event_time;
    }
}
