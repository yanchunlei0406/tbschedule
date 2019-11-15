package com.taobao.pamirs.schedule.zk;

public class ScheduleWatcher{
//
//    private static transient Logger log = LoggerFactory.getLogger(ScheduleWatcher.class);
//    private Map<String, Watcher> route = new ConcurrentHashMap<String, Watcher>();
//    private ZKManager manager;
//
//    public ScheduleWatcher(ZKManager aManager) {
//        this.manager = aManager;
//    }
//
//    public void registerChildrenChanged(String path, Watcher watcher) throws Exception {
//        manager.getZooKeeper().getChildren().forPath(path);
//        route.put(path, watcher);
//    }
//
//    public void process(WatchedEvent event) {
//        if (log.isInfoEnabled()) {
//            log.info("已经触发了" + event.getType() + ":" + event.getState() + "事件！" + event.getPath());
//        }
//        if (event.getType() == Event.EventType.NodeChildrenChanged) {
//            String path = event.getPath();
//            Watcher watcher = route.get(path);
//            if (watcher != null) {
//                try {
//                    watcher.process(event);
//                } finally {
//                    try {
//                        if (manager.getZooKeeper().checkExists().forPath(path) != null) {
//                            manager.getZooKeeper().getChildren().forPath(path);
//                        }
//                    } catch (Exception e) {
//                        log.error(path + ":" + e.getMessage(), e);
//                    }
//                }
//            } else {
//                log.info("已经触发了" + event.getType() + ":" + event.getState() + "事件！" + event.getPath());
//            }
//        } else if (event.getState() == KeeperState.AuthFailed) {
//            log.info("tb_hj_schedule zk status =KeeperState.AuthFailed！");
//        } else if (event.getState() == KeeperState.ConnectedReadOnly) {
//            log.info("tb_hj_schedule zk status =KeeperState.ConnectedReadOnly！");
//        } else if (event.getState() == KeeperState.Disconnected) {
//            log.info("tb_hj_schedule zk status =KeeperState.Disconnected！");
//            try {
//                manager.reConnection();
//            } catch (Exception e) {
//                log.error(e.getMessage(), e);
//            }
//        } else if (event.getState() == KeeperState.NoSyncConnected) {
//            log.info("tb_hj_schedule zk status =KeeperState.NoSyncConnected！等待重新建立ZK连接.. ");
//            try {
//                manager.reConnection();
//            } catch (Exception e) {
//                log.error(e.getMessage(), e);
//            }
//        } else if (event.getState() == KeeperState.SaslAuthenticated) {
//            log.info("tb_hj_schedule zk status =KeeperState.SaslAuthenticated！");
//        } else if (event.getState() == KeeperState.Unknown) {
//            log.info("tb_hj_schedule zk status =KeeperState.Unknown！");
//        } else if (event.getState() == KeeperState.SyncConnected) {
//            log.info("收到ZK连接成功事件！");
//        } else if (event.getState() == KeeperState.Expired) {
//            log.error("会话超时，等待重新建立ZK连接...");
//            try {
//                manager.reConnection();
//            } catch (Exception e) {
//                log.error(e.getMessage(), e);
//            }
//        }
//    }
}