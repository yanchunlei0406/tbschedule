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
        this.filterConfig=filterConfig;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        String name=filterConfig.getInitParameter("name");
        String cipher=filterConfig.getInitParameter("cipher");
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
        if (userName!=null&&userCipher!=null&&userName.equals(name)&&userCipher.equals(cipher)) {
            response.setDateHeader("Expires", 10);
            chain.doFilter(request, response);
        } else { //未通过验证
            response.setStatus(401); //设置好相应的状态
            response.setHeader("WWW-Authenticate", "Basic realm=\"My Application\"");

            //设置用户取消验证后的消息提示
            response.setContentType("text/html; charset=UTF-8");
            response.getWriter().print("请输入合法的授权认证信息!");
        }
    }

    @Override
    public void destroy() {

    }
}
