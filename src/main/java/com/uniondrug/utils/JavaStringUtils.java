package com.uniondrug.utils;

/**
 * java通用字符串校验
 * @author RWang
 * @Date 2020/12/17
 */

public class JavaStringUtils {

    public static boolean isEmpty(String str) {
        return null == str || 0 == str.trim().length();
    }

    /**
     * 过滤字符
     * @param str
     * @param filterStr
     * @param replaceStr
     * @return
     */
    public static String replaceStrAll(String str, String filterStr, String replaceStr) {
        if (!isEmpty(str) && !isEmpty(filterStr)) {
            return str.replaceAll(filterStr, replaceStr);
        }
        return str;
    }

    /**
     * 按长度截取
     * @param str
     * @param len
     * @return
     */
    public static String subStrByLength(String str, int len) {
        if (!isEmpty(str) && len < str.length()) {
            return str.substring(0, len);
        }
        return str;
    }
}
