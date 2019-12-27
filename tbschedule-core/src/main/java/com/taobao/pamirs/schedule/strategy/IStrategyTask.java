package com.taobao.pamirs.schedule.strategy;

public interface IStrategyTask {

    public void initialTaskParameter(String strategyName, String taskParameter) throws Exception;
    /**
     * 当服务器停止的时候，调用此方法清除所有未处理任务，清除服务器的注册信息。<br>
     * 也可能是控制中心发起的终止指令。<br> 
     * 需要注意的是，这个方法必须在当前任务处理完毕后才能执行
     */
    public void stop(String strategyName) throws Exception;
}
