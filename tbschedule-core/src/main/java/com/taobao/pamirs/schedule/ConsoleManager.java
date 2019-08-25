package com.taobao.pamirs.schedule;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.strategy.TBScheduleManagerFactory;
import com.taobao.pamirs.schedule.taskmanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ZKManager;

public class ConsoleManager {

	protected static transient Logger log = LoggerFactory.getLogger(ConsoleManager.class);

	public final static String configFile = System.getProperty("user.dir") + File.separator
			+ "pamirsScheduleConfig.properties";

	// private static TBScheduleManagerFactory scheduleManagerFactory;

	private static HashMap<Integer, TBScheduleManagerFactory> factoryMap = new HashMap<>();

	public static TBScheduleManagerFactory getFactory(HttpServletRequest request) {
		String sessionId = request.getSession().getId();
		Properties p = (Properties) SessionPool.getSession(sessionId);
		return factoryMap.get(p.hashCode());
	}

	public static boolean isInitial(HttpServletRequest request) throws Exception {
		return getFactory(request) != null;
	}

	public static boolean initial(HttpServletRequest request) throws Exception {
		if (getFactory(request) != null) {
			return true;
		}
		Properties p = (Properties) SessionPool.getSession(request==null?null:request.getSession().getId());
		if (p != null) {
			TBScheduleManagerFactory scheduleManagerFactory = new TBScheduleManagerFactory();
			scheduleManagerFactory.start = false;
			factoryMap.put(p.hashCode(), scheduleManagerFactory);
			// Console不启动调度能力
//            Properties p = new Properties();
//            FileReader reader = new FileReader(file);
//            p.load(reader);
//            reader.close();
			scheduleManagerFactory.init(p);
//            log.info("加载Schedule配置文件：" + configFile);
			log.info("从properties中读取Schedule配置" + p);
			return true;
		} else {
			return false;
		}
	}

	public static TBScheduleManagerFactory getScheduleManagerFactory(HttpServletRequest request) throws Exception {
		if (isInitial(request) == false) {
			initial(request);
		}
		return getFactory(request);
	}

	public static IScheduleDataManager getScheduleDataManager(HttpServletRequest request) throws Exception {
		if (isInitial(request) == false) {
			initial(request);
		}
		return getFactory(request).getScheduleDataManager();
	}

	public static ScheduleStrategyDataManager4ZK getScheduleStrategyManager(HttpServletRequest request) throws Exception {
		if (isInitial(request) == false) {
			initial(request);
		}
		return getFactory(request).getScheduleStrategyManager();
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

	public static void saveConfigInfo(HttpServletRequest request, Properties p) throws Exception {
		String sessionId = request.getSession().getId();
		try {
			if (SessionPool.getSession(sessionId) != null) {
				SessionPool.setSession(sessionId, p);
				getFactory(request).reInit(p);
			} else {
				SessionPool.setSession(sessionId, p);
				initial(request);
			}
			// 保存配置信息时，将当前配置信息和会话绑定到一起
			SessionPool.setSession(request.getSession().getId(), p);
//            FileWriter writer = new FileWriter(configFile);
//            p.store(writer, "");
//            writer.close();
		} catch (Exception ex) {
			throw new Exception("不能写入配置信息到文件：" + configFile, ex);
		}
		if (getFactory(request) == null) {
			initial(request);
		} else {
			getFactory(request).reInit(p);
		}
	}

	public static void setScheduleManagerFactory(Properties p,TBScheduleManagerFactory scheduleManagerFactory) {
		//ConsoleManager.scheduleManagerFactory = scheduleManagerFactory;
		factoryMap.put(p.hashCode(), scheduleManagerFactory);
	}

}
