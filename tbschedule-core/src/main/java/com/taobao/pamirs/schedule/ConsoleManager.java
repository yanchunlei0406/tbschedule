package com.taobao.pamirs.schedule;

import com.taobao.pamirs.schedule.strategy.TBScheduleManagerFactory;
import com.taobao.pamirs.schedule.taskmanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ZKManager;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleManager {

    protected static transient Logger log = LoggerFactory.getLogger(ConsoleManager.class);

    public final static String configFile =
        System.getProperty("user.dir") + File.separator + "pamirsScheduleConfig.properties";

    private static TBScheduleManagerFactory scheduleManagerFactory;
    
    public static boolean isInitial() throws Exception {
        return scheduleManagerFactory != null;
    }

    public static boolean initial(String sessionId) throws Exception {
    	SessionPool.getSession();
        if (scheduleManagerFactory != null) {
            return true;
        }
        if(StringUtils.isNotEmpty(sessionId)&&SessionPool.getSession(sessionId)!=null) {
        	scheduleManagerFactory.init((Properties)SessionPool.getSession(sessionId));
        	log.info("从会话{}中加载配置项",sessionId);
        }
        File file = new File(configFile);
        scheduleManagerFactory = new TBScheduleManagerFactory();
        scheduleManagerFactory.start = false;

        if (file.exists() == true) {
            // Console不启动调度能力
            Properties p = new Properties();
            FileReader reader = new FileReader(file);
            p.load(reader);
            reader.close();
            scheduleManagerFactory.init(p);
            log.info("加载Schedule配置文件：" + configFile);
            return true;
        } else {
            return false;
        }
    }

    public static TBScheduleManagerFactory getScheduleManagerFactory() throws Exception {
        if (isInitial() == false) {
            initial();
        }
        return scheduleManagerFactory;
    }

    public static IScheduleDataManager getScheduleDataManager() throws Exception {
        if (isInitial() == false) {
            initial();
        }
        return scheduleManagerFactory.getScheduleDataManager();
    }

    public static ScheduleStrategyDataManager4ZK getScheduleStrategyManager() throws Exception {
        if (isInitial() == false) {
            initial();
        }
        return scheduleManagerFactory.getScheduleStrategyManager();
    }

    public static Properties loadConfig() throws IOException {
        File file = new File(configFile);
        Properties properties;
        if (file.exists() == false) {
            properties = ZKManager.createProperties();
        } else {
            properties = new Properties();
            FileReader reader = new FileReader(file);
            properties.load(reader);
            reader.close();
        }
        return properties;
    }

    public static void saveConfigInfo(HttpServletRequest request,Properties p) throws Exception {
        try {
        	if(SessionPool.getSession(request.getSession().getId())!=null) {
        		SessionPool.setSession(request.getSession().getId(),p);
        		scheduleManagerFactory.reInit(p);
        	}else {
        		SessionPool.setSession(request.getSession().getId(),p);
        		initial(request.getSession().getId());
        	}
            //保存配置信息时，将当前配置信息和会话绑定到一起
            SessionPool.setSession(request.getSession().getId(),p);
            FileWriter writer = new FileWriter(configFile);
            p.store(writer, "");
            writer.close();
        } catch (Exception ex) {
            throw new Exception("不能写入配置信息到文件：" + configFile, ex);
        }
        if
        if (scheduleManagerFactory == null) {
            initial(request.getSession().getId());
        } else {
            scheduleManagerFactory.reInit(p);
        }
    }

    public static void setScheduleManagerFactory(TBScheduleManagerFactory scheduleManagerFactory) {
        ConsoleManager.scheduleManagerFactory = scheduleManagerFactory;
    }

}
