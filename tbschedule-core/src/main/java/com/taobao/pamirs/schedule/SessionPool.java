package com.taobao.pamirs.schedule;
import java.util.*;

import javax.servlet.http.HttpSession;

/**
 * 存放session对象，key为每个session对应的令牌，为vb登录服务.
 * 
 * @author liuyingjie 
 * 
 */
public class SessionPool {
	private static HashMap sessionPool = new HashMap();

	/**
	 * 获得session
	 * 
	 * @param key
	 *            令牌
	 * @return Object session对象
	 */
	public static Object getSession(String key) {
		return sessionPool.get(key);
	}
	public static HashMap getSession() {
		return sessionPool;
	}
	/**
	 * 把session对象放到pool中去
	 * 
	 * @param key
	 *            令牌
	 * @param session
	 *            session对象
	 */
	public static void setSession(String key, Object session) {
		sessionPool.put(key, session);
	}

	/**
	 * 根据key把session移走
	 * 
	 * @param key
	 *            令牌
	 */

	public static void remove(String key) {
		sessionPool.remove(key);
	}

	/**
	 * 移走指定的session
	 * 
	 * @param session
	 *            要移除的session对象
	 */
	public static void remove(Object session) {
		HttpSession hSession = null;
		try {
			hSession = (HttpSession) session;
		} catch (ClassCastException cce) {
			throw new IllegalArgumentException(
				"session "
					+ session.getClass().getName()
					+ " must be of type HttpSession!");
		}
		Iterator iter = sessionPool.keySet().iterator();
		while (iter.hasNext()) {
			String key = (String) iter.next();
			HttpSession httpSession = (HttpSession) sessionPool.get(key);
			if (httpSession.getId().trim().equals(hSession.getId().trim())) {
				sessionPool.remove(key);
			}
		}
	}

	/**
	 * 获得会话池的大小
	 * 
	 * @return int 会话池的大小
	 */
	public static int size() {
		return sessionPool.size();
	}

}
