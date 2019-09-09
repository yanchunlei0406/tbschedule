package com.taobao.pamirs.schedule.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

import com.taobao.pamirs.schedule.taskmanager.MicroServer;

public class ScheduleMicroserverDataManager4ZK {
	//private static transient Logger log = LoggerFactory.getLogger(ScheduleMicroserverDataManager4ZK.class);
	private ZKManager zkManager;
	private String MicroPath;

	public ScheduleMicroserverDataManager4ZK(ZKManager zkManager) throws Exception {
		this.zkManager = zkManager;
		String rootPath = this.zkManager.getRootPath();
		this.MicroPath = rootPath.substring(0, rootPath.lastIndexOf("/")) + "/micro";
		if (this.getZooKeeper().exists(this.MicroPath, false) == null) {
			ZKTools.createPath(getZooKeeper(), this.MicroPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
		}
	}

	private ZooKeeper getZooKeeper() throws Exception {
		return this.zkManager.getZooKeeper();
	}

	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	public List<MicroServer> loadAllMicroServer() throws Exception {
		String zkPath = this.MicroPath;
		List<MicroServer> result = new ArrayList<MicroServer>();
		List<String> names = this.getZooKeeper().getChildren(zkPath, false);
		Collections.sort(names);
		for (String name : names) {
			result.add(this.loadMicro(name));
		}
		return result;
	}
	public MicroServer loadMicro(String microName) throws Exception {
		return this.loadMicro(microName, false);
	}
	/**
	 * 
	 * 
	 * @param microName
	 * @param isDecode 是否需要对微服务名称进行解码处理,前台调用需要 
	 * @return
	 * @throws Exception
	 */
	public MicroServer loadMicro(String microName,boolean isDecode) throws Exception {
		if(isDecode){
			microName=new String(microName.trim().toString().getBytes("iso8859-1"),"utf-8");
		}
		String zkPath = this.MicroPath + "/" + microName;
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			return null;
		}
		MicroServer result = new MicroServer();
		result.setMicroName(microName);
		byte[] microValue = this.getZooKeeper().getData(zkPath, false, null);
		result.setMicroValue(new String(microValue));
		return result;
	}

	/**
	 * 
	 * @param ms
	 * @throws Exception
	 */
	public void createMicro(MicroServer ms) throws Exception {
		String msName=new String(ms.getMicroName().trim().toString().getBytes("iso8859-1"),"utf-8");
		String zkPath = this.MicroPath + "/" + msName;
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, ms.getMicroValue().trim().getBytes(), this.zkManager.getAcl(),
					CreateMode.PERSISTENT);
		} else {
			throw new Exception("微服务【" + msName + "】已经存在,如果确认需要重建，请先删除");
		}
	}

	/**
	 * 
	 * @param ms
	 * @throws Exception
	 */
	public void updateMicro(MicroServer ms, String microNameOlad) throws Exception {
		try {
			String msName=new String(ms.getMicroName().trim().toString().getBytes("iso8859-1"),"utf-8");
			microNameOlad=new String(microNameOlad.trim().toString().getBytes("iso8859-1"),"utf-8");
			String zkPath = this.MicroPath + "/" + msName;
			if (!msName.equals(microNameOlad)) {
				if(this.getZooKeeper().exists(zkPath, false) != null){
					throw new Exception("已存在相同的微服务名称！");
				}
				deleteMicro(microNameOlad);
			}
			String valueString = ms.getMicroValue();
			if (this.getZooKeeper().exists(zkPath, false) == null) {
				this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),
						CreateMode.PERSISTENT);
			} else {
				this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}

	/**
	 * 
	 * @param taskType
	 * @throws Exception
	 */
	public void deleteMicro(String microName) throws Exception {
		try {
			microName=new String(microName.trim().toString().getBytes("iso8859-1"),"utf-8");
			String zkPath = this.MicroPath + "/" + microName;
			if (this.getZooKeeper().exists(zkPath, false) != null) {
				ZKTools.deleteTree(this.getZooKeeper(), zkPath);
			}
		} catch (Exception e) {
			throw new Exception("删除失败！");
		}
	}
}
