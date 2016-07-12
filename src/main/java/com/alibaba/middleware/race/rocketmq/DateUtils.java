package com.alibaba.middleware.race.rocketmq;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DateUtils {

    /**
     * unix时间戳转换为dateFormat
     * 
     * @param beginDate
     * @return
     */
    public static String timestampToDate(String beginDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String sd = sdf.format(new Date(Long.parseLong(beginDate)));
        return sd;
    }
    
public static void main(String[] args) {
	String string= DateUtils.timestampToDate("1465223621641");
	System.out.println(string);
}
    /**
     * 自定义格式时间戳转换
     * 
     * @param beginDate
     * @return
     */
    public static String timestampToDate(String beginDate,String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        String sd = sdf.format(new Date(Long.parseLong(beginDate)));
        return sd;
    }

    /**
     * 将字符串转为时间戳
     * 
     * @param user_time
     * @return
     */
    public static String dateToTimestamp(String user_time) {
        String re_time = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        Date d;
        try {
            d = sdf.parse(user_time);
            long l = d.getTime();
            String str = String.valueOf(l);
            re_time = str.substring(0, 10);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return re_time;
    }

}