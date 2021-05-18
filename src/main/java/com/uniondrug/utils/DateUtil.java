package com.uniondrug.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * DateUtil
 *
 * @author wangrui
 * @date 2021/01/13
 */
public class DateUtil {
    private static Logger LOG = LoggerFactory.getLogger(DateUtil.class);

    private static final String BEFORE = "before";

    private static final String AFTER = "after";

    private static final String HOLIDAY = "holiday";

    private static final String WEEKDAY = "weekDay";

    private static final String WEEKEND = "weekEnd";

    private static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    private static final String YYYY_MM_DD = "yyyy-MM-dd";

    private static final int ONE = 1;

    private static final int SEVEN = 7;

    /**
     * 获取时间的格式化
     * @param date
     * @param format
     * @return
     */
    public static String getFormatNow(long date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    /**
     * 计算工作日(向前或向后滑动n天)
     * @param dateStr
     * @param dayLen
     * @param holiday
     * @param weekDay
     * @param slideType
     * @return
     */
    public static String getPlanDay(String dateStr, int dayLen, String holiday, String weekDay, String slideType) {
        if (!JavaStringUtils.isEmpty(dateStr) && 0 < dayLen) {
            String result = "";
            if (!JavaStringUtils.isEmpty(slideType)) {
                result = getSlideWeekday(dateStr, holiday, weekDay, slideType);
            } else {
                return dateStr;
            }
            int tmpLen = dayLen - 1;
            if (0 < tmpLen) {
                return getPlanDay(result, tmpLen, holiday, weekDay, slideType);
            } else {
                return result;
            }
        }
        return dateStr;
    }

    /**
     * 计算工作日
     * @param dateStr
     * @param holiday
     * @param weekDay
     * @param slideType
     * @return
     */
    public static String getSlideWeekday(String dateStr, String holiday, String weekDay, String slideType) {
        String tmpDateStr = dateStr;
        try {
            SimpleDateFormat sdf =  new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(sdf.parse(dateStr));
            calendar.set(Calendar.DATE, BEFORE.equals(slideType) ? calendar.get(Calendar.DATE) - 1 : calendar.get(Calendar.DATE) + 1);
            tmpDateStr = sdf.format(calendar.getTime());
            String dayType = checkDayType(tmpDateStr, holiday, weekDay);
            if (HOLIDAY.equals(dayType) || WEEKEND.equals(dayType)) {
                return getSlideWeekday(tmpDateStr, holiday, weekDay, slideType);
            }
        } catch (Exception e) {
            LOG.error("DateUtil.getSlideWeekday: ", e);
        }
        return tmpDateStr;
    }

    /**
     * 判断是否是工作日
     * @param dateStr
     * @param holiday
     * @param weekDay
     * @return
     */
    public static String checkDayType(String dateStr, String holiday, String weekDay) {
        try {
            SimpleDateFormat sdf =  new SimpleDateFormat(YYYY_MM_DD);
            String tmpDate = sdf.format(sdf.parse(dateStr));
            if (holiday.contains(tmpDate)) {
                return HOLIDAY;
            } else if (weekDay.contains(tmpDate)) {
                return WEEKDAY;
            }

            SimpleDateFormat sdf1 =  new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(sdf1.parse(dateStr));
            int dayIndex = calendar.get(Calendar.DAY_OF_WEEK);
            if (ONE == dayIndex || SEVEN == dayIndex) {
                return WEEKEND;
            }
        } catch (Exception e) {
            LOG.error("DateUtil.checkDayType: ", e);
        }

        return WEEKDAY;
    }

    /**
     * 两个时间之间的天数
     *
     * @param date1
     * @param date2
     * @return
     */
    public static long getDays(Date date1, Date date2) {
        if (null == date1 || null == date2) {
            return 0;
        }
        // 转换为标准时间
        return getTwoDayTimes(date1, date2) / (24 * 60 * 60 * 1000);
    }

    /**
     * 得到二个日期间的间隔毫秒数 sj1-sj2
     * @param sj1
     * @param sj2
     * @return
     */
    public static long getTwoDayTimes(Date sj1, Date sj2) {
        long day = 0;
        try {
            day = sj1.getTime() - sj2.getTime();
        } catch (Exception e) {
            LOG.error("DateUtil.getTwoDayTimes: ", e);
        }
        return day;
    }

    /*
     * 将时间转换为时间戳
     */
    public static String dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }
}
