package cn.lfar.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 1. 用户行为数据表
 * <p>
 * <p>
 * <p>
 * | userId | itemid | categoryId | behavior | timestamp |
 * | ------ | ------ | ---------- | -------- | --------- |
 * | 000001 | 000001 | 01         | buy      |           |
 * <p>
 * userid是用户ID，itemid是商品ID，categoryId是商品类别ID，behavior是行为类型比如(购买buy，浏览pv等)，timestamp就是时间戳。
 * 问题： 每隔5分钟输出最近一小时内点击量最多的前N个商品，热门度点击量用浏览次数（“pv”）来衡量
 */
public class HotItemTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(env, environmentSettings);
        //获取数据
        ste.executeSql("create table user_behavior(\n" +
                "\tuserId string,\n" +
                "\titemid string,\n" +
                "\tcategoryId string,\n" +
                "\tbehavior string,\n" +
                "\t`timestamp` bigint,\n" +
                "\teventTime as to_timestamp(from_unixtime(`timestamp`,'yyyy-MM-dd HH:mm:ss')),\n" +
                "\twatermark for eventTime as eventTime - interval '5' second\n" +
                ") with (\n" +
                "\t'connector'='jdbc',\n" +
                "\t'url'='jdbc:mysql://bigdata03:3306/mydb?useSSL=false',\n" +
                "\t'driver'='com.mysql.jdbc.Driver',\n" +
                "\t'table-name'='user_behavior',\n" +
                "\t'username'='root',\n" +
                "\t'password'='123456'\n" +
                ")");
        //计算每个窗口的商品点击量
        Table t1 = ste.sqlQuery("select *,\n" +
                "row_number() over(partition by w_start order by itemCount desc) rk \n" +
                "from(\n" +
                "\tselect \n" +
                "\titemid,\n" +
                "\thop_start(eventTime,interval '5' minute,interval '1' hour) w_start,\n" +
                "\tcount(*) itemCount\n" +
                "\tfrom user_behavior\n" +
                "\twhere behavior = 'pv'\n" +
                "\tgroup by hop(eventTime,interval '5' minute,interval '1' hour),itemid\n" +
                ")");
        ste.createTemporaryView("t1", t1);

        //查询前三
        Table t3 = topN(ste, 3);

        //打印到控制台
        ste.executeSql("create table hot_item(\n" +
                "\titemid string,\n" +
                "\tw_start string,\n" +
                "\titemCount bigint,\n" +
                "\trk bigint\n" +
                ") with (\n" +
                "\t'connector'='print'\n" +
                ")");
        t3.insertInto("hot_item");


    }

    private static Table topN(StreamTableEnvironment ste, int n) {

        return ste.sqlQuery("select itemid,w_start,itemCount,rk from t1 where rk<=" + n);

    }
}
