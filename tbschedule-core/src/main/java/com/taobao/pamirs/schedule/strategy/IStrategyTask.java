package com.taobao.pamirs.schedule.strategy;
/**
<<<<<<< HEAD
 * 调度服务器<br>
=======
>>>>>>> branch 'develop' of https://github.com/yanchunlei0406/tbschedule.git
 * 每个分片对应一个IStrategyTask实例<br>
 * 每个IStrategyTask实例代表一个线程组<br>
 * @author Administrator
 *
 */
public interface IStrategyTask {

    public void initialTaskParameter(String strategyName, String taskParameter) throws Exception;

    public void stop(String strategyName) throws Exception;
}
