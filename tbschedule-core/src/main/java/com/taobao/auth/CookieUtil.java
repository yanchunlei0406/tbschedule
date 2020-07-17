package com.taobao.auth;

import org.apache.commons.lang.StringUtils;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * TODO
 *
 * @author ycl
 * @date 2020/7/17
 */
public class CookieUtil {

    /**
     * 设置cookie
     *
     * @param response
     * @param name
     *            cookie名字
     * @param value
     *            cookie值
     * @param maxAge
     *            cookie生命周期 以秒为单位
     */
    public static void addCookie(HttpServletResponse response, String name, String value, int maxAge) {
        try {
            Cookie cookie = new Cookie(name, value);
            if (maxAge > 0)
                cookie.setMaxAge(maxAge);
            cookie.setPath("/");
            response.addCookie(cookie);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 获取指定cookies的值 findCookieByName
     *
     * @param request
     * @param name
     * @return String
     */
    public static String findCookieByName(HttpServletRequest request, String name) {
        Cookie[] cookies = request.getCookies();
        if (null == cookies || cookies.length == 0)
            return null;
        String string = null;
        try {
            for (int i = 0; i < cookies.length; i++) {
                Cookie cookie = cookies[i];
                String cname = cookie.getName();
                if (!StringUtils.isBlank(cname) && cname.equals(name)) {
                    string = cookie.getValue();
                }

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return string;
    }
}
