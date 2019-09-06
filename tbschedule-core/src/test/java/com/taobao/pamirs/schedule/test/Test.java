package com.taobao.pamirs.schedule.test;

public class Test {
	@org.junit.Test
	public void pathTest() {
		String path="/schedule/ecp/microserver/sync";
		String microPath=path.substring(0,path.lastIndexOf("/"));
		System.out.println(microPath);
	}
}
