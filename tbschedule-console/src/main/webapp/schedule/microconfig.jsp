<%@page import="com.taobao.pamirs.schedule.ConsoleManager"%>
<%@page import="com.taobao.pamirs.schedule.taskmanager.MicroServer"%>
<%@page import="java.util.List"%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%
	if(ConsoleManager.isInitial(request) == false){
		response.sendRedirect("config.jsp");
	}
%>
<html>
<head>
<title>
Schedule调度管理
</title>
<STYLE type=text/css>

TH{height:20px;color:#5371BA;font-weight:bold;font-size:12px;text-align:center;border:#8CB2E3 solid;border-width:0 1 1 0;background-color:#E4EFF1;}
TD{background-color: ;border:#8CB2E3 1px solid;border-width:0 1 1 0;font-size:12px;}
table{border-collapse:collapse}
</STYLE>

</head>
<body style="font-size:12px;">
<table id="list" border="1" >
<thead>
     <tr>
     	<th>序号</th>
     	<th >管理</th>
     	<th>微服务名称</th>
     	<th>存储目录</th>
     </tr>
     </thead>
     <tbody>
<%
	List<MicroServer> micros = ConsoleManager.getScheduleMicroserverManager(request).loadAllMicroServer();
	for (int i = 0; i < micros.size(); i++) {
%>
     <tr>
     	<td><%=(i + 1)%></td>
     	<td width="120" align="center">
     		<a target="microDetail" href="microEdit.jsp?microName=<%=micros.get(i).getMicroName()%>"  style="color:#0000CD">编辑</a>
     		<a target="microDetail" href="javascript:void(0)" onclick="validateDel('<%=micros.get(i).getMicroName()%>')" style="color:#0000CD">删除</a>
     	</td>
     	<td><%=micros.get(i).getMicroName()%></td>
     	<td><%=micros.get(i).getMicroValue()%></td>
     </tr>
<%
	}
%>
</tbody>
</table>
<br/>

<a target="microDetail" href="microEdit.jsp?taskType=-1"  style="color:#0000CD">创建微服务...</a>
<br/>
<iframe id="microDetail" name="microDetail"  height="80%" width="100%"></iframe>
</body>
</html>
<script>
function deleteTaskType(baseTaskType){
	//return window.confirm("请确认所有的调度器都已经停止，否则会导致调度器异常！");
		
}

function validateDel(str) {
    var flag = window.confirm("确认删除选中的微服务配置信息？");
    if(flag) {
        window.location.href="microDeal.jsp?action=deleteMicro&microName="+str; 
    }
}
</script>