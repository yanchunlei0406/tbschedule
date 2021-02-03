package com.taobao.pamirs.schedule.strategy;

import com.taobao.pamirs.schedule.ConsoleManager;
import com.taobao.pamirs.schedule.IScheduleTaskDeal;
import com.taobao.pamirs.schedule.ScheduleUtil;
import com.taobao.pamirs.schedule.taskmanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.taskmanager.TBScheduleManagerStatic;
import com.taobao.pamirs.schedule.zk.ScheduleDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ZKManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * 调度服务器构造器
 *
 * @author xuannan
 */
public class TBScheduleManagerFactory implements ApplicationContextAware {

    protected static transient Logger logger = LoggerFactory.getLogger(TBScheduleManagerFactory.class);

    private Map<String, String> zkConfig;

    protected ZKManager zkManager;

    /**
     *	 是否启动调度管理，如果只是做系统管理，应该设置为false
     * 	从ConsoleManager过来的对象就是false，不做为调度管理器
     */
    public boolean start = true;
    private int timerInterval = 2000;
    /**
     * ManagerFactoryTimerTask上次执行的时间戳。<br/> zk环境不稳定，可能导致所有task自循环丢失，调度停止。<br/> 外层应用，通过jmx暴露心跳时间，监控这个tbschedule最重要的大循环。<br/>
     */
    public volatile long timerTaskHeartBeatTS = System.currentTimeMillis();

    /**
                * 调度配置中心客服端
     */
    private IScheduleDataManager scheduleDataManager;
    private ScheduleStrategyDataManager4ZK scheduleStrategyManager;
    /**
     * key: 策略名称<br>
     * 
     * value:任务调度分配器
     */
    private Map<String, List<IStrategyTask>> managerMap = new ConcurrentHashMap<String, List<IStrategyTask>>();

    private ApplicationContext applicationcontext;
    private String uuid;
    private String ip;
    private String hostName;

    /**
     * 	初始化tBScheduleManagerFactory过程中会实体化timer和timerTask
     * 	每2秒执行一次的定时任务
     * 	当stopAll()和restart()时才会停止调度并销毁
     */
    private Timer timer;
    private ManagerFactoryTimerTask timerTask;
    
    
    protected Lock lock = new ReentrantLock();

    volatile String errorMessage = "No config Zookeeper connect infomation";
    private InitialThread initialThread;

    public TBScheduleManagerFactory() {
        this.ip = ScheduleUtil.getLocalIP();
        this.hostName = ScheduleUtil.getLocalHostName();
    }

    public void init() throws Exception {
        Properties properties = new Properties();
        for (Map.Entry<String, String> e : this.zkConfig.entrySet()) {
            properties.put(e.getKey(), e.getValue());
        }
        this.init(properties);
    }

    public void reInit(Properties p) throws Exception {
        if (this.start == true || this.timer != null || this.managerMap.size() > 0) {
            throw new Exception("调度器有任务处理，不能重新初始化");
        }
        this.init(p);
    }

    public void init(Properties p) throws Exception {
        if (this.initialThread != null) {
            this.initialThread.stopThread();
        }
        this.lock.lock();
        try {
            this.scheduleDataManager = null;
            this.scheduleStrategyManager = null;
            //ConsoleManager.scheduelManagerFactory对象
            ConsoleManager.setScheduleManagerFactory(this);
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            this.zkManager = new ZKManager(p);
            this.errorMessage = "Zookeeper connecting ......" + this.zkManager.getConnectStr();
            initialThread = new InitialThread(this);
            initialThread.setName("TBScheduleManagerFactory-initialThread");
            System.out.println("init开始");
            initialThread.start();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 在Zk状态正常后回调数据初始化
     */
    public void initialData() throws Exception {
        //RootPATH初始维护
        this.zkManager.initial();
        //baseTaskType初始维护
        this.scheduleDataManager = new ScheduleDataManager4ZK(this.zkManager);
        //strategy，factory初始维护
        this.scheduleStrategyManager = new ScheduleStrategyDataManager4ZK(this.zkManager);
        if (this.start == true) {
            /**
             *  	注册(维护)调度管理器
             *          factory/factoryUUID
             *  	维护策略分配节点
             *          strategy/strategyName/factoryUUID
             */
            this.scheduleStrategyManager.registerManagerFactory(this);
            if (timer == null) {
                timer = new Timer("TBScheduleManagerFactory-Timer");
            }
            if (timerTask == null) {
                /**
                 * 	心跳线程：
                 *      	优先级最高,每2秒执行一次
                 *      	检测zk连接状态，当连续5个心跳周期后zk仍然没有恢复正常连接，一切重新开始，完成闭环循环。
                 *      	当zk连接正常，进行核心工作流程: this.factory.refresh()
                 */
                timerTask = new ManagerFactoryTimerTask(this);
                timer.schedule(timerTask, 2000, this.timerInterval);
            }
        }
    }

    /**
     * 创建调度服务器
     * @param strategy 策略信息 /strategy/strategyName    
     * @author ycl 2019年12月23日
     */
    public IStrategyTask createStrategyTask(ScheduleStrategy strategy) throws Exception {
        IStrategyTask result = null;
        try {
            if (ScheduleStrategy.Kind.Schedule == strategy.getKind()) {
                String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(strategy.getTaskName());
                String ownSign = ScheduleUtil.splitOwnsignFromTaskType(strategy.getTaskName());
                result = new TBScheduleManagerStatic(this, baseTaskType, ownSign, scheduleDataManager);
            } else if (ScheduleStrategy.Kind.Java == strategy.getKind()) {
                result = (IStrategyTask) Class.forName(strategy.getTaskName()).newInstance();
                result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
            } else if (ScheduleStrategy.Kind.Bean == strategy.getKind()) {
                result = (IStrategyTask) this.getBean(strategy.getTaskName());
                result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
            }
        } catch (Exception e) {
            logger.error("strategy 获取对应的java or bean 出错,schedule并没有加载该任务,请确认" + strategy.getStrategyName(), e);
        }
        return result;
    }

    /**
     * 
     * 1:判断服务器信息是否停止
     *   服务器停止,服务器上的所有调度器需要标记销毁:TODO
     * 2：重新分配调度器(/strategy/taskName/factoryUUID)
     *   
    * @Description: TODO
    * @throws Exception   
    * @return void   
    * @author ycl 2019年12月13日
     */
    public void refresh() throws Exception {
        this.lock.lock();
        try {
            //判断状态是否终止
            ManagerFactoryInfo stsInfo = null;
            boolean isException = false;
            try {
                //获取服务器信息
                stsInfo = this.getScheduleStrategyManager().loadManagerFactoryInfo(this.getUuid());
            } catch (Exception e) {
                /**
                 * zk的机器管理中，获取当前任务处理机信息异常
                 * 1：任务管理器不存在
                 * 2：zk连接异常
                 */
                isException = true;
                logger.error("获取服务器信息有误：uuid=" + this.getUuid(), e);
            }
            /**
             * 	异常时要注销该服务器上所有调度任务
             */
            if (isException == true) {
                try {
                    // 停止所有的调度任务
                    stopServer(null);
                    // (删除/strategy/taskName/factoryUUID节点)
                    this.getScheduleStrategyManager().unRregisterManagerFactory(this);
                } finally {
                    reRegisterManagerFactory();
                }
            } else if (stsInfo.isStart() == false) {
                stopServer(null); // 停止所有的调度任务
                this.getScheduleStrategyManager().unRregisterManagerFactory(this);
            } else {
                reRegisterManagerFactory();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 重新分配调度器:
     * 1:遍历要注销的策略名称进行任务注销
     * 2:获取当前服务器可执行策略的信息集合: /strategy/strategyName/factoryUUID
     * 3:
     * @Description: TODO
     * @throws Exception   
     * @return void   
     * @author ycl 2019年12月23日
     */
    public void reRegisterManagerFactory() throws Exception {
        List<String> stopList = this.getScheduleStrategyManager().registerManagerFactory(this);
        //根据不再运行的策略名称查找对应的任务调度器集合->终止运行
        for (String strategyName : stopList) {
            this.stopServer(strategyName);
        }
        //重新分配调度任务的机器
        this.assignScheduleServer();
        this.reRunScheduleServer();
    }

    /**
     * 根据策略重新分配调度任务的机器<br>
     * （更新请求的服务器数量 /strategy/strategyName/factoryUUID --> requestNum）<br>
     * 由于factoryList是排序的，请求服务器数量一般不会发生变化<br>
     */
    public void assignScheduleServer() throws Exception {
        //返回当前factory可执行的策略信息集合 /strategy/strategyName/factoryUUID
        //遍历集合
        for (ScheduleStrategyRunntime run : this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.uuid)) {
            // 根据策略名称，返回策略下的所有factory运行时信息
            // /strategy/strategyName/*
            List<ScheduleStrategyRunntime> factoryList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByTaskType(run.getStrategyName());
            // uuid最小的为Leader节点，只有Leader节点可以分配调度任务
            if (factoryList.size() == 0 || this.isLeader(this.uuid, factoryList) == false) {
                continue;
            }
            //获取当前策略的运行时信息 /strategy/strategyName
            ScheduleStrategy scheduleStrategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
            //分配任务数量
            int[] nums = ScheduleUtil.assignTaskNumber(factoryList.size(), scheduleStrategy.getAssignNum(), scheduleStrategy.getNumOfSingleServer());
            for (int i = 0; i < factoryList.size(); i++) {
                ScheduleStrategyRunntime factory = factoryList.get(i);
                // 更新每个服务器分配到的分片数量
                this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(run.getStrategyName(), factory.getUuid(), nums[i]);
            }
        }
    }

    public boolean isLeader(String uuid, List<ScheduleStrategyRunntime> factoryList) {
        try {
            long no = Long.parseLong(uuid.substring(uuid.lastIndexOf("$") + 1));
            for (ScheduleStrategyRunntime server : factoryList) {
                if (no > Long.parseLong(server.getUuid().substring(server.getUuid().lastIndexOf("$") + 1))) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("判断Leader出错：uuif=" + uuid, e);
            return true;
        }
    }
    /**
     * 停止/增加调度器
     * @return    
     * @author ycl 2019年12月23日
     */
    public void reRunScheduleServer() throws Exception {
        //返回当前factory可执行的策略信息集合 /strategy/strategyName/factoryUUID
        for (ScheduleStrategyRunntime run : this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.uuid)) {
            //根据策略名称获取任务调度分配器
            List<IStrategyTask> list = this.managerMap.get(run.getStrategyName());
            if (list == null) {
                list = new ArrayList<IStrategyTask>();
                this.managerMap.put(run.getStrategyName(), list);
            }
            while (list.size() > run.getRequestNum() && list.size() > 0) {
                IStrategyTask task = list.remove(list.size() - 1);
                try {
                    task.stop(run.getStrategyName());
                } catch (Throwable e) {
                    logger.error("注销任务错误：strategyName=" + run.getStrategyName(), e);
                }
            }
            // 不足，增加调度器
            ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
            while (list.size() < run.getRequestNum()) {
                //根据策略信息创建调度服务器
                IStrategyTask result = this.createStrategyTask(strategy);
                if (null == result) {
                    logger.error("strategy 对应的配置有问题。strategy name=" + strategy.getStrategyName());
                }
                list.add(result);
            }
        }
    }

    /**
     * 终止一类任务
     */
    public void stopServer(String strategyName) throws Exception {
        if (strategyName == null) {
            String[] nameList = (String[]) this.managerMap.keySet().toArray(new String[0]);
            for (String name : nameList) {
                for (IStrategyTask task : this.managerMap.get(name)) {
                    try {
                        task.stop(strategyName);
                    } catch (Throwable e) {
                        logger.error("注销任务错误：strategyName=" + strategyName, e);
                    }
                }
                this.managerMap.remove(name);
            }
        } else {
            List<IStrategyTask> list = this.managerMap.get(strategyName);
            if (list != null) {
                for (IStrategyTask task : list) {
                    try {
                        task.stop(strategyName);
                    } catch (Throwable e) {
                        logger.error("注销任务错误：strategyName=" + strategyName, e);
                    }
                }
                this.managerMap.remove(strategyName);
            }

        }
    }

    /**
     * 停止所有调度资源
     */
    public void stopAll() throws Exception {
        try {
            lock.lock();
            this.start = false;
            if (this.initialThread != null) {
                this.initialThread.stopThread();
            }
            if (this.timer != null) {
                if (this.timerTask != null) {
                    this.timerTask.cancel();
                    this.timerTask = null;
                }
                this.timer.cancel();
                this.timer = null;
            }
            this.stopServer(null);
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            if (this.scheduleStrategyManager != null) {
                try {
                    ZooKeeper zk = this.scheduleStrategyManager.getZooKeeper();
                    if (zk != null) {
                        zk.close();
                    }
                } catch (Exception e) {
                    logger.error("stopAll zk getZooKeeper异常！", e);
                }
            }
            this.uuid = null;
            logger.info("stopAll 停止服务成功！");
        } catch (Throwable e) {
            logger.error("stopAll 停止服务失败：" + e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 重启所有的服务
     */
    public void reStart() throws Exception {
        try {
            if (this.timer != null) {
                if (this.timerTask != null) {
                    this.timerTask.cancel();
                    this.timerTask = null;
                }
                this.timer.purge();
            }
            this.stopServer(null);
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            this.uuid = null;
            this.init();
        } catch (Throwable e) {
            logger.error("重启服务失败：" + e.getMessage(), e);
        }
    }

    public boolean isZookeeperInitialSucess() throws Exception {
        return this.zkManager.checkZookeeperState();
    }

    public String[] getScheduleTaskDealList() {
        return applicationcontext.getBeanNamesForType(IScheduleTaskDeal.class);

    }

    public IScheduleDataManager getScheduleDataManager() {
        if (this.scheduleDataManager == null) {
            throw new RuntimeException(this.errorMessage);
        }
        return scheduleDataManager;
    }

    public ScheduleStrategyDataManager4ZK getScheduleStrategyManager() {
        if (this.scheduleStrategyManager == null) {
            throw new RuntimeException(this.errorMessage);
        }
        return scheduleStrategyManager;
    }

    @Override
    public void setApplicationContext(ApplicationContext aApplicationcontext) throws BeansException {
        applicationcontext = aApplicationcontext;
    }

    public Object getBean(String beanName) {
        return applicationcontext.getBean(beanName);
    }

    public String getUuid() {
        return uuid;
    }

    public String getIp() {
        return ip;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getHostName() {
        return hostName;
    }

    public void setStart(boolean isStart) {
        this.start = isStart;
    }

    public void setTimerInterval(int timerInterval) {
        this.timerInterval = timerInterval;
    }

    public void setZkConfig(Map<String, String> zkConfig) {
        this.zkConfig = zkConfig;
    }

    public ZKManager getZkManager() {
        return this.zkManager;
    }

    public Map<String, String> getZkConfig() {
        return zkConfig;
    }
}

class ManagerFactoryTimerTask extends java.util.TimerTask {

    private static transient Logger log = LoggerFactory.getLogger(ManagerFactoryTimerTask.class);
    TBScheduleManagerFactory factory;
    int count = 0;

    public ManagerFactoryTimerTask(TBScheduleManagerFactory aFactory) {
        this.factory = aFactory;
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            if (this.factory.zkManager.checkZookeeperState() == false) {
                //连续5次（10s）没有连接到zk，就尝试销毁一切，重新初始化tbScheduleFactory
            	if (count > 5) {
                    log.error("Zookeeper连接失败，关闭所有的任务后，重新连接Zookeeper服务器......");
                    this.factory.reStart();

                } else {
                    count = count + 1;
                }
            } else {
            	//zk连接正常时，定时(2s)进行数据检测、更新
                count = 0;
                this.factory.refresh();
            }

        } catch (Throwable ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            factory.timerTaskHeartBeatTS = System.currentTimeMillis();
        }
    }
}

class InitialThread extends Thread {

    private static transient Logger log = LoggerFactory.getLogger(InitialThread.class);
    TBScheduleManagerFactory facotry;
    boolean isStop = false;

    public InitialThread(TBScheduleManagerFactory aFactory) {
        log.info("调用initThread构造方法...");
        this.facotry = aFactory;
    }

    public void stopThread() {
        log.info("调用initThread线程停止...");
        this.isStop = true;
    }

    @Override
    public void run() {
        facotry.lock.lock();
        try {
            log.info("initThread线程开始...");
            int count = 0;
            while (facotry.zkManager.checkZookeeperState() == false) {
                count = count + 1;
                if (count % 50 == 0) {
                    facotry.errorMessage = "Zookeeper connecting ......" + facotry.zkManager.getConnectStr() + " spendTime:" + count * 20 + "(ms)";
                    log.error(facotry.errorMessage);
                }
                Thread.sleep(20);
                if (this.isStop == true) {
                    log.info("initThread线程停止...");
                    return;
                }
            }
            facotry.initialData();
        } catch (Throwable e) {
            log.info("initThread线程异常...");
            log.error(e.getMessage(), e);
        } finally {
            facotry.lock.unlock();
        }

    }

}