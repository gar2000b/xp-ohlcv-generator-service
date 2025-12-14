package ca.digilogue.xp.model;

public class Info {
    private String appName;
    private String version;
    private String instanceId;

    public Info(String appName, String version, String instanceId) {
        this.appName = appName;
        this.version = version;
        this.instanceId = instanceId;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }
}