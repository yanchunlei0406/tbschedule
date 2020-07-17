package com.taobao.auth;

import sun.misc.BASE64Decoder;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * TODO
 *
 * @author ycl
 * @date 2020/7/16
 */
public class AuthFilter implements Filter {
    private FilterConfig filterConfig;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        this.filterConfig = filterConfig;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        String name = filterConfig.getInitParameter("name");
        String cipher = filterConfig.getInitParameter("cipher");
        boolean needAuth = false;
        //获取到的内容是结果base64编码后的字符串，所以这样的认证方式安全性不高
        String authValue = request.getHeader("Authorization"), //获取到的请求头格式类似于 Basic MTIzOjEyMw==
                userName = null,
                userCipher = null;
        if (authValue != null) {
            BASE64Decoder decoder = new BASE64Decoder();
            String[] values = new String(decoder.decodeBuffer(authValue.split(" ")[1])).split(":"); //通过解析后的用户名和密码格式例如 123:123
            if (values.length == 2) {
                userName = values[0];
                userCipher = values[1];
            }
        }
        //用户名和密码都不为空时验证成功
        if (userName != null && userCipher != null && userName.equals(name) && userCipher.equals(cipher)) {
            //cookie中的lastAuthTime主要是判断用户最后一次操作时间，如果超过10分钟没有操作，会重新进行密码验证
            Object obj = CookieUtil.findCookieByName(request, "lastAuthTime");
            long lastAuthTime = obj == null ? 0 : Long.valueOf(obj.toString());
            long currentTime = System.currentTimeMillis();
//            System.out.println("currentTime: " + currentTime + " ,lastAuthTime: " + lastAuthTime + " ;");
            //通过验证
            if (lastAuthTime == 0 || currentTime - lastAuthTime <= 600000) {
//                System.out.println("相差:" + (currentTime - lastAuthTime) + " millis");
                CookieUtil.addCookie(response, "lastAuthTime", String.valueOf(currentTime), -1);
                chain.doFilter(request, response);
            } else {
//                System.out.println("相差:" + (currentTime - lastAuthTime) + " millis");
                needAuth = true;
            }
        } else {
            needAuth = true;
        }
        if (needAuth) {
            //未通过验证
            System.out.println("未通过验证!");
            response.setStatus(401); //设置好相应的状态
            response.setHeader("WWW-Authenticate", "Basic realm=\"My Application\"");
            CookieUtil.addCookie(response, "lastAuthTime", String.valueOf(0), -1);
            //设置用户取消验证后的消息提示
            response.setContentType("text/html; charset=UTF-8");
            response.getWriter().print("请输入合法的授权认证信息!");
        }
    }

    @Override
    public void destroy() {

    }


}
