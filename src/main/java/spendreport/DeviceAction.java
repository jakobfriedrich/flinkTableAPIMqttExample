package spendreport;

public class DeviceAction {
    public Integer actionid;

    public Integer userid;

    public Integer deviceid;

    public String event_name;

    public String event_time;

    // default constructor for DataStream API
    public DeviceAction() {}

    // fully assigning constructor for Table API
    public DeviceAction(Integer actionid, Integer userid, Integer deviceid, String event_name, String event_time) {
        this.actionid = actionid;
        this.userid = userid;
        this.deviceid = deviceid;
        this.event_name = event_name;
        this.event_time = event_time;
    }
}

