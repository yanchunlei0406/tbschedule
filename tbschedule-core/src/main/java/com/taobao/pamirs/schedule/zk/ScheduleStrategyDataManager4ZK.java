package com.taobao.pamirs.schedule.zk;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.taobao.pamirs.schedule.strategy.ManagerFactoryInfo;
import com.taobao.pamirs.schedule.strategy.ScheduleStrategy;
import com.taobao.pamirs.schedule.strategy.ScheduleStrategyRunntime;
import com.taobao.pamirs.schedule.strategy.TBScheduleManagerFactory;
import java.io.Writer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

public class ScheduleStrategyDataManager4ZK {

    private ZKManager zkManager;
    private String PATH_Strategy;
    private String PATH_ManagerFactory;
    private Gson gson;

    // 在Spring对象创建完毕后，创建内部对象
    public ScheduleStrategyDataManager4ZK(ZKManager aZkManager) throws Exception {
        this.zkManager = aZkManager;
        gson = new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();
        this.PATH_Strategy = this.zkManager.getRootPath() + "/strategy";
        this.PATH_ManagerFactory = this.zkManager.getRootPath() + "/factory";

        if (this.getZooKeeper().exists(this.PATH_Strategy, false) == null) {
            ZKTools.createPath(getZooKeeper(), this.PATH_Strategy, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
        if (this.getZooKeeper().exists(this.PATH_ManagerFactory, false) == null) {
            ZKTools.createPath(getZooKeeper(), this.PATH_ManagerFactory, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
    }
    /**
     * 获取/strategy/strategyName 运行时信息
     * @return    
     * @author ycl 2019年12月23日
     */
    public ScheduleStrategy loadStrategy(String strategyName) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            return null;
        }
        String valueString = new String(this.getZooKeeper().getData(zkPath, false, null));
        ScheduleStrategy result = (ScheduleStrategy) this.gson.fromJson(valueString, ScheduleStrategy.class);
        return result;
    }

    public void createScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName();
        String valueString = this.gson.toJson(scheduleStrategy);
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
        } else {
            throw new Exception("调度策略" + scheduleStrategy.getStrategyName() + "已经存在,如果确认需要重建，请先调用deleteMachineStrategy(String taskType)删除");
        }
    }

    public void updateScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName();
        String valueString = this.gson.toJson(scheduleStrategy);
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
        } else {
            this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
        }

    }

    public void deleteMachineStrategy(String taskType) throws Exception {
        deleteMachineStrategy(taskType, false);
    }

    public void pause(String strategyName) throws Exception {
        ScheduleStrategy strategy = this.loadStrategy(strategyName);
        strategy.setSts(ScheduleStrategy.STS_PAUSE);
        this.updateScheduleStrategy(strategy);
    }

    public void resume(String strategyName) throws Exception {
        ScheduleStrategy strategy = this.loadStrategy(strategyName);
        strategy.setSts(ScheduleStrategy.STS_RESUME);
        this.updateScheduleStrategy(strategy);
    }

    public void deleteMachineStrategy(String taskType, boolean isForce) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + taskType;
        if (isForce == false && this.getZooKeeper().getChildren(zkPath, null).size() > 0) {
            throw new Exception("不能删除" + taskType + "的运行策略，会导致必须重启整个应用才能停止失去控制的调度进程。" + "可以先清空IP地址，等所有的调度器都停止后再删除调度策略");
        }
        ZKTools.deleteTree(this.getZooKeeper(), zkPath);
    }

    public List<ScheduleStrategy> loadAllScheduleStrategy() throws Exception {
        String zkPath = this.PATH_Strategy;
        List<ScheduleStrategy> result = new ArrayList<ScheduleStrategy>();
        List<String> names = this.getZooKeeper().getChildren(zkPath, false);
        Collections.sort(names);
        for (String name : names) {
            result.add(this.loadStrategy(name));
        }
        return result;
    }

    /**
     * 1：维护任务处理机tBScheduleManagerFactory:<br>
     *         <b> 初次注册</b><br>
     *                  生成一个唯一的UUID：ip$hostname$uuid，在/factory下注册临时顺序节点<br>
     *                  给当前factory的uuid属性赋值<br>
     *          <b>非初次注册</b><br>
     *                  检测节点是否存在(临时节点有可能因为网络原因丢失)。<br><br>
     * 2：遍历/strategy下所有的策略信息strategyNames<br>
     * 		<b>当前处理机在可运行范围内&&策略是运行状态</b><br>
     * 			维护：/strategy/strategyName/factoryUUID节点<br>
     *      <b>处理机不在可运行范围内</b><br>
     *      	判断：  /strategy/strategyName/factoryUUID节点存在<br>
     *      		如果存在，删除节点，收集strategyName后续终止任务<br>
     *      		如果不存在，表示已经中止任务<br>
     *      	
     *        
     * @return 需要全部注销的调度
     */
    public List<String> registerManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception {

        if (managerFactory.getUuid() == null) {
            String uuid = managerFactory.getIp() + "$" + managerFactory.getHostName() + "$" + UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
            String zkPath = this.PATH_ManagerFactory + "/" + uuid + "$";
            zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
            managerFactory.setUuid(zkPath.substring(zkPath.lastIndexOf("/") + 1));
        } else {
            String zkPath = this.PATH_ManagerFactory + "/" + managerFactory.getUuid();
            if (this.getZooKeeper().exists(zkPath, false) == null) {
                zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL);
            }
        }
        List<String> result = new ArrayList<String>();
        for (ScheduleStrategy scheduleStrategy : loadAllScheduleStrategy()) {
            boolean isFind = false;
            // 恢复状态，且在运行IP范围内，isFind=true
            // 并且要存在 rootPath/strategy/strategyName/factoryUUID
            if (ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(scheduleStrategy.getSts()) == false && scheduleStrategy.getIPList() != null) {
                for (String ip : scheduleStrategy.getIPList()) {
                    if (ip.equals("127.0.0.1") || ip.equalsIgnoreCase("localhost") || ip.equals(managerFactory.getIp()) || ip.equalsIgnoreCase(managerFactory.getHostName())) {
                        // 添加可管理TaskType
                        String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName() + "/" + managerFactory.getUuid();
                        if (this.getZooKeeper().exists(zkPath, false) == null) {
                            zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL);
                        }
                        isFind = true;
                        break;
                    }
                }
            }
            /**
             * 	策略是暂停状态，或者当前任务处理机不在策略的运行IP范围内
             * 	判断rootPath/strategy/strategyName/factoryUUID节点
             * 		1、如果存在 ，就需要删除节点，中止任务stopServer(strategyName)
             * 		2、如果不存在，表示已经中止任务(如果极短时间内停止策略->删除策略及任务，可能出现任务未被中止的问题)
             * 
             */
            if (isFind == false) {// 清除原来注册的Factory
                String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName() + "/" + managerFactory.getUuid();
                if (this.getZooKeeper().exists(zkPath, false) != null) {
                    ZKTools.deleteTree(this.getZooKeeper(), zkPath);
                    result.add(scheduleStrategy.getStrategyName());
                }
            }
        }
        return result;
    }

    /**
     * 注销服务，停止调度
     * (删除/strategy/taskName/factoryUUID节点)
     */
    public void unRregisterManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception {
        for (String taskName : this.getZooKeeper().getChildren(this.PATH_Strategy, false)) {
            String zkPath = this.PATH_Strategy + "/" + taskName + "/" + managerFactory.getUuid();
            if (this.getZooKeeper().exists(zkPath, false) != null) {
                ZKTools.deleteTree(this.getZooKeeper(), zkPath);
            }
        }
    }
    /**
     * 获取节点信息:/strategy/strategyName/factoryUUID
     * @Description: TODO
     * @param strategyName
     * @param uuid
     * @return
     * @throws Exception   
     * @return ScheduleStrategyRunntime   
     * @author ycl 2019年12月23日
     */
    public ScheduleStrategyRunntime loadScheduleStrategyRunntime(String strategyName, String uuid) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + uuid;
        ScheduleStrategyRunntime result = null;
        if (this.getZooKeeper().exists(zkPath, false) != null) {
            byte[] value = this.getZooKeeper().getData(zkPath, false, null);
            if (value != null) {
                String valueString = new String(value);
                result = (ScheduleStrategyRunntime) this.gson.fromJson(valueString, ScheduleStrategyRunntime.class);
                if (null == result) {
                    throw new Exception("gson 反序列化异常,对象为null");
                }
                if (null == result.getStrategyName()) {
                    throw new Exception("gson 反序列化异常,策略名字为null");
                }
                if (null == result.getUuid()) {
                    throw new Exception("gson 反序列化异常,uuid为null");
                }
            } else {
                result = new ScheduleStrategyRunntime();
                result.setStrategyName(strategyName);
                result.setUuid(uuid);
                result.setRequestNum(0);
                result.setMessage("");
            }
        }
        return result;
    }

    /**
     * 装载所有的策略运行状态
     */
    public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntime() throws Exception {
        List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
        String zkPath = this.PATH_Strategy;
        for (String taskType : this.getZooKeeper().getChildren(zkPath, false)) {
            for (String uuid : this.getZooKeeper().getChildren(zkPath + "/" + taskType, false)) {
                result.add(loadScheduleStrategyRunntime(taskType, uuid));
            }
        }
        return result;
    }
   /**
    * 返回当前factory可执行的策略信息集合
    * /strategy/strategyName/factoryUUID
    * @return    
    * @author ycl 2019年12月23日
    */
    public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByUUID(String managerFactoryUUID) throws Exception {
        List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
        // 策略节点/strategy
        String zkPath = this.PATH_Strategy;
        // 节点下是所有策略
        List<String> taskTypeList = this.getZooKeeper().getChildren(zkPath, false);
        // 排序
        Collections.sort(taskTypeList);
        for (String taskType : taskTypeList) {
            //  存在节点/strategy/strategyName/factoryUUID
            if (this.getZooKeeper().exists(zkPath + "/" + taskType + "/" + managerFactoryUUID, false) != null) {
                result.add(loadScheduleStrategyRunntime(taskType, managerFactoryUUID));
            }
        }
        return result;
    }
    /**
     * 获取/strategy/strategyName/* 下所有factoryUUID的运行时信息并返回
     * @return    
     * @author ycl 2019年12月23日
     */
    public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByTaskType(String strategyName) throws Exception {
        List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
        String zkPath = this.PATH_Strategy;
        if (this.getZooKeeper().exists(zkPath + "/" + strategyName, false) == null) {
            return result;
        }
        //获取 /strategy/strategyName/* 下所有的factoryUUID
        List<String> uuidList = this.getZooKeeper().getChildren(zkPath + "/" + strategyName, false);
        // 排序
        Collections.sort(uuidList, new Comparator<String>() {
            public int compare(String u1, String u2) {
                return u1.substring(u1.lastIndexOf("$") + 1).compareTo(u2.substring(u2.lastIndexOf("$") + 1));
            }
        });
        //获取运行时信息
        for (String uuid : uuidList) {
            result.add(loadScheduleStrategyRunntime(strategyName, uuid));
        }
        return result;
    }

    /**
     * 更新请求数量
     * /strategy/strategyName/factoryUUID --> requestNum
     */
    public void updateStrategyRunntimeReqestNum(String strategyName, String manangerFactoryUUID, int requestNum) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + manangerFactoryUUID;
        ScheduleStrategyRunntime result = null;
        if (this.getZooKeeper().exists(zkPath, false) != null) {
            result = this.loadScheduleStrategyRunntime(strategyName, manangerFactoryUUID);
        } else {
            result = new ScheduleStrategyRunntime();
            result.setStrategyName(strategyName);
            result.setUuid(manangerFactoryUUID);
            result.setRequestNum(requestNum);
            result.setMessage("");
        }
        result.setRequestNum(requestNum);
        String valueString = this.gson.toJson(result);
        this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
    }

    /**
     * 更新调度过程中的信息
     */
    public void updateStrategyRunntimeErrorMessage(String strategyName, String manangerFactoryUUID, String message) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + manangerFactoryUUID;
        ScheduleStrategyRunntime result = null;
        if (this.getZooKeeper().exists(zkPath, false) != null) {
            result = this.loadScheduleStrategyRunntime(strategyName, manangerFactoryUUID);
        } else {
            result = new ScheduleStrategyRunntime();
            result.setStrategyName(strategyName);
            result.setUuid(manangerFactoryUUID);
            result.setRequestNum(0);
        }
        result.setMessage(message);
        String valueString = this.gson.toJson(result);
        this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
    }

    public void updateManagerFactoryInfo(String uuid, boolean isStart) throws Exception {
        String zkPath = this.PATH_ManagerFactory + "/" + uuid;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            throw new Exception("任务管理器不存在:" + uuid);
        }
        this.getZooKeeper().setData(zkPath, Boolean.toString(isStart).getBytes(), -1);
    }

    public ManagerFactoryInfo loadManagerFactoryInfo(String uuid) throws Exception {
        String zkPath = this.PATH_ManagerFactory + "/" + uuid;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            throw new Exception("任务管理器不存在:" + uuid);
        }
        byte[] value = this.getZooKeeper().getData(zkPath, false, null);
        ManagerFactoryInfo result = new ManagerFactoryInfo();
        result.setUuid(uuid);
        if (value == null) {
            result.setStart(true);
        } else {
            result.setStart(Boolean.parseBoolean(new String(value)));
        }
        return result;
    }

    /**
     * 导入配置信息【目前支持baseTaskType和strategy数据】
     */
    public void importConfig(String config, Writer writer, boolean isUpdate) throws Exception {
        ConfigNode configNode = gson.fromJson(config, ConfigNode.class);
        if (configNode != null) {
            String path = configNode.getRootPath() + "/" + configNode.getConfigType();
            ZKTools.createPath(getZooKeeper(), path, CreateMode.PERSISTENT, zkManager.getAcl());
            String y_node = path + "/" + configNode.getName();
            if (getZooKeeper().exists(y_node, false) == null) {
                writer.append("<font color=\"red\">成功导入新配置信息\n</font>");
                getZooKeeper().create(y_node, configNode.getValue().getBytes(), zkManager.getAcl(), CreateMode.PERSISTENT);
            } else if (isUpdate) {
                writer.append("<font color=\"red\">该配置信息已经存在，并且强制更新了\n</font>");
                getZooKeeper().setData(y_node, configNode.getValue().getBytes(), -1);
            } else {
                writer.append("<font color=\"red\">该配置信息已经存在，如果需要更新，请配置强制更新\n</font>");
            }
        }
        writer.append(configNode.toString());
    }

    /**
     * 输出配置信息【目前备份baseTaskType和strategy数据】
     */
    public StringBuffer exportConfig(String rootPath, Writer writer) throws Exception {
        StringBuffer buffer = new StringBuffer();
        for (String type : new String[] { "baseTaskType", "strategy" }) {
            if (type.equals("baseTaskType")) {
                writer.write("<h2>基本任务配置列表：</h2>\n");
            } else {
                writer.write("<h2>基本策略配置列表：</h2>\n");
            }
            String bTTypePath = rootPath + "/" + type;
            List<String> fNodeList = getZooKeeper().getChildren(bTTypePath, false);
            for (int i = 0; i < fNodeList.size(); i++) {
                String fNode = fNodeList.get(i);
                ConfigNode configNode = new ConfigNode(rootPath, type, fNode);
                configNode.setValue(new String(this.getZooKeeper().getData(bTTypePath + "/" + fNode, false, null)));
                buffer.append(gson.toJson(configNode));
                buffer.append("\n");
                writer.write(configNode.toString());
            }
            writer.write("\n\n");
        }
        if (buffer.length() > 0) {
            String str = buffer.toString();
            return new StringBuffer(str.substring(0, str.length() - 1));
        }
        return buffer;
    }

    public List<ManagerFactoryInfo> loadAllManagerFactoryInfo() throws Exception {
        String zkPath = this.PATH_ManagerFactory;
        List<ManagerFactoryInfo> result = new ArrayList<ManagerFactoryInfo>();
        List<String> names = this.getZooKeeper().getChildren(zkPath, false);
        Collections.sort(names, new Comparator<String>() {
            public int compare(String u1, String u2) {
                return u1.substring(u1.lastIndexOf("$") + 1).compareTo(u2.substring(u2.lastIndexOf("$") + 1));
            }
        });
        for (String name : names) {
            ManagerFactoryInfo info = new ManagerFactoryInfo();
            info.setUuid(name);
            byte[] value = this.getZooKeeper().getData(zkPath + "/" + name, false, null);
            if (value == null) {
                info.setStart(true);
            } else {
                info.setStart(Boolean.parseBoolean(new String(value)));
            }
            result.add(info);
        }
        return result;
    }

    public void printTree(String path, Writer writer, String lineSplitChar) throws Exception {
        ZKTools.printTree(this.getZooKeeper(), path, writer, lineSplitChar);
    }

    public void deleteTree(String path) throws Exception {
        ZKTools.deleteTree(this.getZooKeeper(), path);
    }

    public ZooKeeper getZooKeeper() throws Exception {
        return this.zkManager.getZooKeeper();
    }

    public String getRootPath() {
        return this.zkManager.getRootPath();
    }
}
