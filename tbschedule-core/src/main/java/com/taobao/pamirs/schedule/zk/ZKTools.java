package com.taobao.pamirs.schedule.zk;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZKTools {

    public static void createPath(CuratorFramework zk, String path, CreateMode createMode, List<ACL> acl) throws Exception {
        zk.create().creatingParentsIfNeeded().withMode(createMode).withACL(acl).forPath(path, null);
    }

    public static String createPath(CuratorFramework zk, String path, byte[] date, List<ACL> acl, CreateMode createMode) throws Exception {
        return zk.create().creatingParentsIfNeeded().withMode(createMode).withACL(acl).forPath(path, date);
    }

    public static void printTree(CuratorFramework zk, String path, Writer writer, String lineSplitChar) throws Exception {
        String[] list = getTree(zk, path);
        Stat stat = new Stat();
        for (String name : list) {
            byte[] value = zk.getData().storingStatIn(stat).forPath(name);
            if (value == null) {
                writer.write(name + lineSplitChar);
            } else {
                writer.write(name + "[v." + stat.getVersion() + "]" + "[" + new String(value) + "]" + lineSplitChar);
            }
        }
    }

    public static void deleteTree(CuratorFramework zk, String path) throws Exception {
        zk.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public static String[] getTree(CuratorFramework zk, String path) throws Exception {
        if (zk.checkExists().forPath(path) == null) {
            return new String[0];
        }
        List<String> dealList = new ArrayList<String>();
        dealList.add(path);
        int index = 0;
        while (index < dealList.size()) {
            String tempPath = dealList.get(index);
            List<String> children = zk.getChildren().forPath(tempPath);
            if (tempPath.equalsIgnoreCase("/") == false) {
                tempPath = tempPath + "/";
            }
            Collections.sort(children);
            for (int i = children.size() - 1; i >= 0; i--) {
                dealList.add(index + 1, tempPath + children.get(i));
            }
            index++;
        }
        return (String[]) dealList.toArray(new String[0]);
    }
}
