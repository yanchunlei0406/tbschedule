<%@page import="com.taobao.pamirs.schedule.ConsoleManager"%>
<%@page import="com.taobao.pamirs.schedule.taskmanager.MicroServer"%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<html>
<head>
<title>
创建微服务
</title>
</head>
<body bgcolor="#ffffff">

<%
	String action = (String) request.getParameter("action");
	String microNameOlad = (String) request.getParameter("microNameOlad");
	String result = "";
	boolean isRefreshParent = false;
	MicroServer micro = new MicroServer();
	try {
		if (action.equalsIgnoreCase("editMicro")
				|| action.equalsIgnoreCase("createMicro")) {
			micro.setMicroName(request.getParameter("microName"));
			micro.setMicroValue(request.getParameter("microValue"));
			if (action.equalsIgnoreCase("createMicro")) {
				ConsoleManager.getScheduleMicroserverManager(request).createMicro(micro);
				isRefreshParent = true;
				result = "创建成功！";
			} else if (action.equalsIgnoreCase("editMicro")) {
				ConsoleManager.getScheduleMicroserverManager(request).updateMicro(micro,microNameOlad);
				isRefreshParent = true;
				result = "修改成功！";
			}
		} else if (action.equalsIgnoreCase("deleteMicro")) {
			micro.setMicroName(request.getParameter("microName"));
			ConsoleManager.getScheduleMicroserverManager(request).deleteMicro(micro.getMicroName());
			isRefreshParent = true;
			result = "删除成功！";
		}
	} catch (Throwable e) {
		e.printStackTrace();
		result = "ERROR:  " + e.getMessage();
		isRefreshParent = false;
	}
%>
<%=result%>
</body>
</html>
<%
	if (isRefreshParent == true) {
%>
<script>
 	parent.location.reload();
</script>
<%
	}
%>