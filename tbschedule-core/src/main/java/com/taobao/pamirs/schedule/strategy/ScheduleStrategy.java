package com.taobao.pamirs.schedule.strategy;

import org.apache.commons.lang.builder.ToStringBuilder;
/**
 * 策略对象
* @Description: TODO
* @author ycl 2019年12月12日
 */
public class ScheduleStrategy {

    public enum Kind {
        Schedule, Java, Bean
    }

    /**
     * 策略名称
     */
    private String strategyName;

    private String[] IPList;

    private int numOfSingleServer;
    /**
     * 指定需要执行调度的机器数量
     */
    private int assignNum;
    /**
     * 任务类型 Schedule, Java, Bean
     */
    private Kind kind;

    /**
     * 任务名称
     */
    private String taskName;
    /**
     * 任务参数
     */
    private String taskParameter;

    /**
     * 服务状态: pause,resume
     */
    private String sts = STS_RESUME;

    public static String STS_PAUSE = "pause";
    public static String STS_RESUME = "resume";

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    public String getStrategyName() {
        return strategyName;
    }

    public void setStrategyName(String strategyName) {
        this.strategyName = strategyName;
    }
    /**
     * 任务项数量
     * @return    
     * @author ycl 2019年12月23日
     */
    public int getAssignNum() {
        return assignNum;
    }

    public void setAssignNum(int assignNum) {
        this.assignNum = assignNum;
    }

    public String[] getIPList() {
        return IPList;
    }

    public void setIPList(String[] iPList) {
        IPList = iPList;
    }

    public void setNumOfSingleServer(int numOfSingleServer) {
        this.numOfSingleServer = numOfSingleServer;
    }
    /**
     * 单服务器允许的最大数量，已废弃使用
     * @return    
     * @author ycl 2019年12月23日
     */
    public int getNumOfSingleServer() {
        return numOfSingleServer;
    }

    public Kind getKind() {
        return kind;
    }

    public void setKind(Kind kind) {
        this.kind = kind;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getTaskParameter() {
        return taskParameter;
    }

    public void setTaskParameter(String taskParameter) {
        this.taskParameter = taskParameter;
    }

    public String getSts() {
        return sts;
    }

    public void setSts(String sts) {
        this.sts = sts;
    }
}
