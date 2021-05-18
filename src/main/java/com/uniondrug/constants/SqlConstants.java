package com.uniondrug.constants;

/**
 * sparkSql的常量
 * @author RWang
 * @Date 2020/12/21
 */

public class SqlConstants {

    /**
     * 推荐结果查询sql
     */
    public static final String RECOMMEND_RESULT_TABLE=
            "select member_id,concat_ws(',',collect_set(internal_id)) as recommend_result from recommend_result_table group by member_id";
}
