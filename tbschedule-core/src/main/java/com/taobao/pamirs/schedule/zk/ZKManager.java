package com.taobao.pamirs.schedule.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKManager {
    private static transient Logger log = LoggerFactory.getLogger(ZKManager.class);
    private CuratorFramework zk;
    private List<ACL> acl = new ArrayList<ACL>();
    private Properties properties;
    private boolean isCheckParentPath = true;

    public enum keys {
        zkConnectString, rootPath, userName, password, zkSessionTimeout, isCheckParentPath
    }

    public ZKManager(Properties aProperties) throws Exception {
        this.properties = aProperties;
        this.connect();
    }

    /**
    * @Title: reConnection  
    * @Description: 重连zookeeper
    * 使用curator后应该没有必要提供该方法，此处暂时保留，监控。
    * @param @throws Exception   
    * @return void   
    * @throws
     */
    public synchronized void reConnection() throws Exception {
        if (this.zk != null) {
            log.info("触发了人为保留的zkReconnection，留意是否有异常出现");
            this.zk.close();
            this.zk = null;
            this.connect();
        }
    }

    private void connect() throws Exception {
        String authString = this.properties.getProperty(keys.userName.toString()) + ":" + this.properties.getProperty(keys.password.toString());
        int sessionTimeout = 60000;
        try {
            sessionTimeout = Integer.valueOf(keys.zkSessionTimeout.toString());
        } catch (Exception e2) {
            // TODO: handle exception
        }
        RetryPolicy retryPolicy = new RetryForever(10000);
        zk = CuratorFrameworkFactory.builder().connectString(this.properties.getProperty(keys.zkConnectString.toString()))
                // .namespace(StringUtils.strip(keys.rootPath.toString(), "/"))
                .retryPolicy(retryPolicy).authorization("digest", authString.getBytes()).sessionTimeoutMs(sessionTimeout).connectionTimeoutMs(10000).canBeReadOnly(false).build();
        zk.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                switch (newState) {
                case CONNECTED:
                    log.info("收到ZK连接成功事件！");
                    log.info("Connection connected: {}", properties.getProperty(keys.zkConnectString.toString()));
                case RECONNECTED:
                    log.info("Connection reconnected: {}", properties.getProperty(keys.zkConnectString.toString()));
                    break;
                case LOST:
                    log.warn("Connection lost: {}", properties.getProperty(keys.zkConnectString.toString()));
                    break;
                case SUSPENDED:
                    log.warn("Connection suspended: {}", properties.getProperty(keys.zkConnectString.toString()));
                    break;
                default:
                    log.warn("Unhandled connection state received: {}", newState);
                    break;
                }
            }
        });
        zk.start();
        acl.clear();

        acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest(authString))));
        acl.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

        zk.setACL().withACL(acl);
        this.isCheckParentPath = Boolean.parseBoolean(this.properties.getProperty(keys.isCheckParentPath.toString(), "true"));
    }

    public void close() throws InterruptedException {
        log.info("关闭zookeeper连接");
        if (zk == null) {
            return;
        }
        this.zk.close();
    }

    public static Properties createProperties() {
        Properties result = new Properties();
        result.setProperty(keys.zkConnectString.toString(), "localhost:2181");
        result.setProperty(keys.rootPath.toString(), "/rootPath/demo");
        result.setProperty(keys.userName.toString(), "");
        result.setProperty(keys.password.toString(), "");
        result.setProperty(keys.zkSessionTimeout.toString(), "60000");
        result.setProperty(keys.isCheckParentPath.toString(), "true");
        return result;
    }

    public String getRootPath() {
        return this.properties.getProperty(keys.rootPath.toString());
    }

    public String getConnectStr() {
        return this.properties.getProperty(keys.zkConnectString.toString());
    }

    public boolean checkZookeeperState() throws Exception {
        return zk != null && zk.getState().equals(CuratorFrameworkState.STARTED);
    }

    public void initial() throws Exception {
        // 当zk状态正常后才能调用
        if (zk.checkExists().forPath(this.getRootPath()) == null) {
            ZKTools.createPath(zk, this.getRootPath(), CreateMode.PERSISTENT, acl);
            if (isCheckParentPath == true) {
                checkParent(zk, this.getRootPath());
            }
            // 设置版本信息
            zk.setData().withVersion(-1).forPath(this.getRootPath(), Version.getVersion().getBytes());
        } else {
            // 先校验父亲节点，本身是否已经是schedule的目录
            if (isCheckParentPath == true) {
                checkParent(zk, this.getRootPath());
            }
            byte[] value = zk.getData().forPath(this.getRootPath());
            if (value == null) {
                zk.setData().withVersion(-1).forPath(this.getRootPath(), Version.getVersion().getBytes());
            } else {
                String dataVersion = new String(value);
                if (Version.isCompatible(dataVersion) == false) {
                    throw new Exception("TBSchedule程序版本 " + Version.getVersion() + " 不兼容Zookeeper中的数据版本 " + dataVersion);
                }
                log.info("当前的程序版本:" + Version.getVersion() + " 数据版本: " + dataVersion);
            }
        }
    }

    public static void checkParent(CuratorFramework zk, String path) throws Exception {
        String[] list = path.split("/");
        String zkPath = "";
        for (int i = 0; i < list.length - 1; i++) {
            String str = list[i];
            if (str.equals("") == false) {
                zkPath = zkPath + "/" + str;
                if (zk.checkExists().forPath(zkPath) != null) {
                    byte[] value = zk.getData().forPath(zkPath);
                    if (value != null) {
                        String tmpVersion = new String(value);
                        if (tmpVersion.indexOf("taobao-pamirs-schedule-") >= 0) {
                            throw new Exception("\"" + zkPath + "\"  is already a schedule instance's root directory, its any subdirectory cannot as the root directory of others");
                        }
                    }
                }
            }
        }
    }

    public List<ACL> getAcl() {
        return acl;
    }

    public CuratorFramework getZooKeeper() throws Exception {
        return this.zk;
    }

}
