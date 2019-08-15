package com.taobao.pamirs.schedule.strategy;
/**
 * zk节点信息<br>
 * zkManagerRootPath()/factory/uuid<br>
 * 当前uuid节点getData为空或者为true时服务正常启动<br>
 * 当前uuid节点getData为false时，停止调度服务<br>
 * @author Administrator
 *
 */
public class ManagerFactoryInfo {

    private String uuid;
    private boolean start;

    public void setStart(boolean start) {
        this.start = start;
    }

    public boolean isStart() {
        return start;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }
}
