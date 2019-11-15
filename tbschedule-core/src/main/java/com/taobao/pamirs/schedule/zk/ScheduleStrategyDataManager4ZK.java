package com.taobao.pamirs.schedule.zk;

import java.io.Writer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.taobao.pamirs.schedule.ConsoleManager;
import com.taobao.pamirs.schedule.strategy.ManagerFactoryInfo;
import com.taobao.pamirs.schedule.strategy.ScheduleStrategy;
import com.taobao.pamirs.schedule.strategy.ScheduleStrategyRunntime;
import com.taobao.pamirs.schedule.strategy.TBScheduleManagerFactory;
import com.taobao.pamirs.schedule.taskmanager.MicroServer;

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

        if (this.getZooKeeper().checkExists().forPath(this.PATH_Strategy) == null) {
            ZKTools.createPath(getZooKeeper(), this.PATH_Strategy, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
        if (this.getZooKeeper().checkExists().forPath(this.PATH_ManagerFactory) == null) {
            ZKTools.createPath(getZooKeeper(), this.PATH_ManagerFactory, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
    }

    public ScheduleStrategy loadStrategy(String strategyName) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            return null;
        }
        String valueString = new String(this.getZooKeeper().getData().forPath(zkPath));
        ScheduleStrategy result = (ScheduleStrategy) this.gson.fromJson(valueString, ScheduleStrategy.class);
        return result;
    }

    public void createScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName();
        String valueString = this.gson.toJson(scheduleStrategy);
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            ZKTools.createPath(getZooKeeper(), zkPath, valueString.getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
        } else {
            throw new Exception("调度策略" + scheduleStrategy.getStrategyName() + "已经存在,如果确认需要重建，请先调用deleteMachineStrategy(String taskType)删除");
        }
    }

    public void updateScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName();
        String valueString = this.gson.toJson(scheduleStrategy);
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            ZKTools.createPath(getZooKeeper(), zkPath, valueString.getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
        } else {
            this.getZooKeeper().setData().withVersion(-1).forPath(zkPath, valueString.getBytes());
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
        if (isForce == false && this.getZooKeeper().getChildren().forPath(zkPath).size() > 0) {
            throw new Exception("不能删除" + taskType + "的运行策略，会导致必须重启整个应用才能停止失去控制的调度进程。" + "可以先清空IP地址，等所有的调度器都停止后再删除调度策略");
        }
        ZKTools.deleteTree(this.getZooKeeper(), zkPath);
    }

    public List<ScheduleStrategy> loadAllScheduleStrategy() throws Exception {
        String zkPath = this.PATH_Strategy;
        List<ScheduleStrategy> result = new ArrayList<ScheduleStrategy>();
        List<String> names = this.getZooKeeper().getChildren().forPath(zkPath);
        Collections.sort(names);
        for (String name : names) {
            result.add(this.loadStrategy(name));
        }
        return result;
    }

    /**
     * 注册ManagerFactory
     *
     * @return 需要全部注销的调度，例如当IP不在列表中
     */
    public List<String> registerManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception {

        if (managerFactory.getUuid() == null) {
            String uuid = managerFactory.getIp() + "$" + managerFactory.getHostName() + "$" + UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
            String zkPath = this.PATH_ManagerFactory + "/" + uuid + "$";
            zkPath = ZKTools.createPath(getZooKeeper(), zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
            managerFactory.setUuid(zkPath.substring(zkPath.lastIndexOf("/") + 1));
        } else {
            String zkPath = this.PATH_ManagerFactory + "/" + managerFactory.getUuid();
            if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
                zkPath = ZKTools.createPath(getZooKeeper(), zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL);
            }
        }

        List<String> result = new ArrayList<String>();
        for (ScheduleStrategy scheduleStrategy : loadAllScheduleStrategy()) {
            boolean isFind = false;
            // 暂停或者不在IP范围
            if (ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(scheduleStrategy.getSts()) == false && scheduleStrategy.getIPList() != null) {
                for (String ip : scheduleStrategy.getIPList()) {
                    if (ip.equals("127.0.0.1") || ip.equalsIgnoreCase("localhost") || ip.equals(managerFactory.getIp()) || ip.equalsIgnoreCase(managerFactory.getHostName())) {
                        // 添加可管理TaskType
                        String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName() + "/" + managerFactory.getUuid();
                        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
                            zkPath = ZKTools.createPath(getZooKeeper(), zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL);
                        }
                        isFind = true;
                        break;
                    }
                }
            }
            if (isFind == false) {// 清除原来注册的Factory
                String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName() + "/" + managerFactory.getUuid();
                if (this.getZooKeeper().checkExists().forPath(zkPath) != null) {
                    ZKTools.deleteTree(this.getZooKeeper(), zkPath);
                    result.add(scheduleStrategy.getStrategyName());
                }
            }
        }
        return result;
    }

    /**
     * 注销服务，停止调度
     */
    public void unRregisterManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception {
        for (String taskName : this.getZooKeeper().getChildren().forPath(this.PATH_Strategy)) {
            String zkPath = this.PATH_Strategy + "/" + taskName + "/" + managerFactory.getUuid();
            if (this.getZooKeeper().checkExists().forPath(zkPath) != null) {
                ZKTools.deleteTree(this.getZooKeeper(), zkPath);
            }
        }
    }

    public ScheduleStrategyRunntime loadScheduleStrategyRunntime(String strategyName, String uuid) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + uuid;
        ScheduleStrategyRunntime result = null;
        if (this.getZooKeeper().checkExists().forPath(zkPath) != null) {
            byte[] value = this.getZooKeeper().getData().forPath(zkPath);
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
        for (String taskType : this.getZooKeeper().getChildren().forPath(zkPath)) {
            for (String uuid : this.getZooKeeper().getChildren().forPath(zkPath + "/" + taskType)) {
                result.add(loadScheduleStrategyRunntime(taskType, uuid));
            }
        }
        return result;
    }

    public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByUUID(String managerFactoryUUID) throws Exception {
        List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
        String zkPath = this.PATH_Strategy;

        List<String> taskTypeList = this.getZooKeeper().getChildren().forPath(zkPath);
        Collections.sort(taskTypeList);
        for (String taskType : taskTypeList) {
            if (this.getZooKeeper().checkExists().forPath(zkPath + "/" + taskType + "/" + managerFactoryUUID) != null) {
                result.add(loadScheduleStrategyRunntime(taskType, managerFactoryUUID));
            }
        }
        return result;
    }

    public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByTaskType(String strategyName) throws Exception {
        List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
        String zkPath = this.PATH_Strategy;
        if (this.getZooKeeper().checkExists().forPath(zkPath + "/" + strategyName) == null) {
            return result;
        }
        List<String> uuidList = this.getZooKeeper().getChildren().forPath(zkPath + "/" + strategyName);
        // 排序
        Collections.sort(uuidList, new Comparator<String>() {
            public int compare(String u1, String u2) {
                return u1.substring(u1.lastIndexOf("$") + 1).compareTo(u2.substring(u2.lastIndexOf("$") + 1));
            }
        });

        for (String uuid : uuidList) {
            result.add(loadScheduleStrategyRunntime(strategyName, uuid));
        }
        return result;
    }

    /**
     * 更新请求数量
     */
    public void updateStrategyRunntimeReqestNum(String strategyName, String manangerFactoryUUID, int requestNum) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + manangerFactoryUUID;
        ScheduleStrategyRunntime result = null;
        if (this.getZooKeeper().checkExists().forPath(zkPath) != null) {
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
        this.getZooKeeper().setData().withVersion(-1).forPath(zkPath, valueString.getBytes());
    }

    /**
     * 更新调度过程中的信息
     */
    public void updateStrategyRunntimeErrorMessage(String strategyName, String manangerFactoryUUID, String message) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + manangerFactoryUUID;
        ScheduleStrategyRunntime result = null;
        if (this.getZooKeeper().checkExists().forPath(zkPath) != null) {
            result = this.loadScheduleStrategyRunntime(strategyName, manangerFactoryUUID);
        } else {
            result = new ScheduleStrategyRunntime();
            result.setStrategyName(strategyName);
            result.setUuid(manangerFactoryUUID);
            result.setRequestNum(0);
        }
        result.setMessage(message);
        String valueString = this.gson.toJson(result);
        this.getZooKeeper().setData().withVersion(-1).forPath(zkPath, valueString.getBytes());
    }

    public void updateManagerFactoryInfo(String uuid, boolean isStart) throws Exception {
        String zkPath = this.PATH_ManagerFactory + "/" + uuid;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            throw new Exception("任务管理器不存在:" + uuid);
        }
        this.getZooKeeper().setData().withVersion(-1).forPath(zkPath, Boolean.toString(isStart).getBytes());
    }

    public ManagerFactoryInfo loadManagerFactoryInfo(String uuid) throws Exception {
        String zkPath = this.PATH_ManagerFactory + "/" + uuid;
        if (this.getZooKeeper().checkExists().forPath(zkPath) == null) {
            throw new Exception("任务管理器不存在:" + uuid);
        }
        byte[] value = this.getZooKeeper().getData().forPath(zkPath);
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
            if (getZooKeeper().checkExists().forPath(y_node) == null) {
                writer.append("<font color=\"red\">成功导入新配置信息\n</font>");
                ZKTools.createPath(getZooKeeper(), y_node, configNode.getValue().getBytes(), zkManager.getAcl(), CreateMode.PERSISTENT);
            } else if (isUpdate) {
                writer.append("<font color=\"red\">该配置信息已经存在，并且强制更新了\n</font>");
                getZooKeeper().setData().withVersion(-1).forPath(y_node, configNode.getValue().getBytes());
            } else {
                writer.append("<font color=\"red\">该配置信息已经存在，如果需要更新，请配置强制更新\n</font>");
            }
        }
        writer.append(configNode.toString());
    }

    /**
     * 输出配置信息【目前备份baseTaskType和strategy数据】
     */
    public StringBuffer exportConfig(HttpServletRequest request, String rootPath, Writer writer, String isMicroserver) throws Exception {
        Map<String, String> pathMap = new HashMap<>();
        if (isMicroserver != null && (isMicroserver.equals("2") || isMicroserver.equals("3"))) {
            List<MicroServer> microList = ConsoleManager.getScheduleMicroserverManager(request).loadAllMicroServer();
            if (microList.size() == 0) {
                // 注册微服务数量为0
                return null;
            }
            for (MicroServer micro : microList) {
                pathMap.put(micro.getMicroName(), micro.getMicroValue());
            }
        } else {
            pathMap.put("指定路径", rootPath);
        }
        StringBuffer buffer = new StringBuffer();
        Iterator<Entry<String, String>> iterators = pathMap.entrySet().iterator();
        while (iterators.hasNext()) {
            Entry<String, String> iterator = iterators.next();
            String name = iterator.getKey();
            String path = iterator.getValue();
            writer.write("<font size=3 color='red'><strong>" + name + "  ==  " + path + "</strong></font>\n");
            for (String type : new String[] { "baseTaskType", "strategy" }) {
                if (type.equals("baseTaskType")) {
                    writer.write("<h4>基本任务配置列表：</h4>");
                } else {
                    writer.write("<h4>基本策略配置列表：</h4>");
                }
                String bTTypePath = path + "/" + type;
                List<String> fNodeList = getZooKeeper().getChildren().forPath(bTTypePath);
                for (int i = 0; i < fNodeList.size(); i++) {
                    String fNode = fNodeList.get(i);
                    ConfigNode configNode = new ConfigNode(path, type, fNode);
                    configNode.setValue(new String(this.getZooKeeper().getData().forPath(bTTypePath + "/" + fNode)));
                    buffer.append(gson.toJson(configNode));
                    buffer.append("\n");
                    writer.write(configNode.toString());
                }
                writer.write("\n\n");
            }
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
        List<String> names = this.getZooKeeper().getChildren().forPath(zkPath);
        Collections.sort(names, new Comparator<String>() {
            public int compare(String u1, String u2) {
                return u1.substring(u1.lastIndexOf("$") + 1).compareTo(u2.substring(u2.lastIndexOf("$") + 1));
            }
        });
        for (String name : names) {
            ManagerFactoryInfo info = new ManagerFactoryInfo();
            info.setUuid(name);
            byte[] value = this.getZooKeeper().getData().forPath(zkPath + "/" + name);
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

    public CuratorFramework getZooKeeper() throws Exception {
        return this.zkManager.getZooKeeper();
    }

    public String getRootPath() {
        return this.zkManager.getRootPath();
    }
}
