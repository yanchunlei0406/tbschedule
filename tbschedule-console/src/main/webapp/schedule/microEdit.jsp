<%@page import="com.taobao.pamirs.schedule.ConsoleManager"%>
<%@page import="com.taobao.pamirs.schedule.taskmanager.MicroServer"%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%
    String isManager= request.getParameter("manager");
	String microName= request.getParameter("microName");
	MicroServer micro =  ConsoleManager.getScheduleMicroserverManager(request).loadMicro(microName);
    boolean isNew = false;
    String actionName ="editMicro";
	if(micro == null){
		micro = new MicroServer();
		micro.setMicroName("");
		micro.setMicroValue("");
		isNew = true;
		actionName ="createMicro";
	}
%>
<html>
<head>
<STYLE type=text/css>

TH{color:#5371BA;font-weight:bold;font-size:12px;background-color:#E4EFF1;display:block;}
TD{font-size:12px;}

</STYLE>
</head>
<body>
<form id="microForm" method="get" name="microForm" action="microDeal.jsp">
<input type="hidden" name="action" value="<%=actionName%>"/>
<input type="hidden" name="microNameOlad" value="<%=microName%>"/>
<table>
<tr>
	<td>微服务名称:</td>
	<td><input type="text" id="microName" name="microName"   value="<%=micro.getMicroName()%>" width="30"></td>
	<td></td>
</tr>
<tr>
	<td>存储目录:</td>
	<td><input type="text" id="microValue" name="microValue"  value="<%=micro.getMicroValue()%>" width="30"></td>
	<td>只能为英文字母</td>
</tr>
</table>
<br/>
<input type="button" value="保存" onclick="save();" style="width:100px" >

</form>

</body>
</html>

<script>
function save(){
	var microName = document.all("microName").value;
	var microValue = document.all("microValue").value;
	var reg = /.*[\u4e00-\u9fa5]+.*$/; 
	if(reg.test(microValue)){
	   alert('存储目录不能含中文');
	   return;
	}
	if(microName==null||microName==''||isContainSpace(microName)){
		alert('微服务名称不能为空或存在空格');
		return;
	}
    document.getElementById("microForm").submit();
}
  
function isContainSpace(array) {   
	if(array.indexOf(' ')>=0){
		return true;
	}
    return false;
}
</script>