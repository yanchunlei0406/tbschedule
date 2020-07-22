package com.taobao.pamirs.schedule;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.strategy.TBScheduleManagerFactory;
import com.taobao.pamirs.schedule.taskmanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.taskmanager.MicroServer;
import com.taobao.pamirs.schedule.zk.ScheduleMicroserverDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ZKManager;

public class ConsoleManager {

	protected static transient Logger log = LoggerFactory.getLogger(ConsoleManager.class);

	public final static String configFile = System.getProperty("user.dir") + File.separator
			+ "pamirsScheduleConfig.properties";
    public enum keys {
        zkConnectString, rootPath, userName, password, zkSessionTimeout, isCheckParentPath
    }
//	private static TBScheduleManagerFactory scheduleManagerFactory;

	private static HashMap<Integer, TBScheduleManagerFactory> factoryMap = new HashMap<>();

	public static TBScheduleManagerFactory getFactory(HttpServletRequest request) {
		String sessionId = request.getSession().getId();
		Properties p = (Properties) SessionPool.getSession(sessionId);
		return factoryMap.get(p == null ? null : p.hashCode());
	}

	public static boolean isInitial(HttpServletRequest request) throws Exception {
		return getFactory(request) != null;
	}

	public static boolean goInital(HttpServletRequest request, MicroServer m) throws Exception {
		if (request == null) {
			return false;
		}
		try {
			String microName=m.getMicroName().trim().toString();
			MicroServer micro=ConsoleManager.getScheduleMicroserverManager(request).loadMicro(microName);
			if(micro==null){
                microName=new String(m.getMicroName().trim().toString().getBytes("iso8859-1"),"utf-8");
                micro=ConsoleManager.getScheduleMicroserverManager(request).loadMicro(microName);
            }
			// 要跳转的微服务在Micro中
			String goRootPath = micro.getMicroValue();
			// request中有现在的properti配置
			// 1:取出当前会话的properti
			Properties p = (Properties) SessionPool.getSession(request.getSession().getId());
			String rootPath = p.getProperty(keys.rootPath.toString());
			if (goRootPath.trim().equals(rootPath.trim())) {
				// 没有变化，不需要刷新跳转
				return true;
			}
			Properties gop = new Properties(p);
			gop.setProperty(keys.rootPath.toString(), goRootPath);
			// 微服务对应的工厂已经存在
			if (factoryMap.containsKey(gop.hashCode())) {
				// 当前会话指向
				SessionPool.setSession(request.getSession().getId(), gop);
				return true;
			}
			// 微服务对应的工厂实例不存在
			TBScheduleManagerFactory scheduleManagerFactory = new TBScheduleManagerFactory();
			scheduleManagerFactory.start = false;
			SessionPool.setSession(request.getSession().getId(), gop);
			scheduleManagerFactory.init(gop);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	public static boolean initial(HttpServletRequest request) throws Exception {
		if (request == null || SessionPool.getSession(request == null ? null : request.getSession().getId()) == null) {
			File file = new File(configFile);
			TBScheduleManagerFactory scheduleManagerFactory = new TBScheduleManagerFactory();
			scheduleManagerFactory.start = false;
			if (file.exists() == true) {
				// Console不启动调度能力
				Properties p = new Properties();
				if(request!=null) {
					SessionPool.setSession(request.getSession().getId(), p);
//					factoryMap.put(p.hashCode(), scheduleManagerFactory);
				}
				FileReader reader = new FileReader(file);
				p.load(reader);
				reader.close();
				scheduleManagerFactory.init(p);
				log.info("加载Schedule配置文件：" + configFile);
				return true;
			} else {
				return false;
			}
		} else {
			if (getFactory(request) != null) {
				return true;
			}
			Properties p = (Properties) SessionPool.getSession(request == null ? null : request.getSession().getId());
			if (p != null) {
				TBScheduleManagerFactory scheduleManagerFactory = new TBScheduleManagerFactory();
				scheduleManagerFactory.start = false;
//				factoryMap.put(p.hashCode(), scheduleManagerFactory);
				scheduleManagerFactory.init(p);
				log.info("从properties中读取Schedule配置" + p);
				return true;
			} else {
				return false;
			}
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

	public static ScheduleStrategyDataManager4ZK getScheduleStrategyManager(HttpServletRequest request)
			throws Exception {
		if (isInitial(request) == false) {
			initial(request);
		}
		return getFactory(request).getScheduleStrategyManager();
	}
	public static ScheduleMicroserverDataManager4ZK getScheduleMicroserverManager(HttpServletRequest request)
			throws Exception {
		if (isInitial(request) == false) {
			initial(request);
		}
		return getFactory(request).getScheduleMicroserverManager();
	}

	public static Properties loadConfig(HttpServletRequest request) throws IOException {
		Properties p = (Properties) SessionPool.getSession(request.getSession().getId());
		if (p != null) {
			return p;
		}
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
			if (getFactory(request) != null) {
				SessionPool.setSession(sessionId, p);
				getFactory(request).reInit(p);
			} else {
				SessionPool.setSession(sessionId, p);
				initial(request);
			}
			// 保存配置信息时，将当前配置信息和会话绑定到一起
			SessionPool.setSession(request.getSession().getId(), p);
            //更新文件中的配置信息
			FileWriter writer = new FileWriter(configFile);
            p.store(writer, "");
            writer.close();
		} catch (Exception ex) {
			throw new Exception("不能写入配置信息到文件：" + configFile, ex);
		}
//		if (getFactory(request) == null) {
//			initial(request);
//		} else {
//			getFactory(request).reInit(p);
//		}
	}

	public static void setScheduleManagerFactory(Properties p, TBScheduleManagerFactory scheduleManagerFactory) {
		// ConsoleManager.scheduleManagerFactory = scheduleManagerFactory;
		factoryMap.put(p.hashCode(), scheduleManagerFactory);
	}

}
