package cn.lfar.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ## 需求二：页面浏览数统计
 *
 * 即统计在一段时间内用户访问某个url的次数，输出某段时间内访问量最多的前N个URL。
 *
 * 如每隔5秒，输出最近10分钟内访问量最多的前N个URL。
 *
 * 2. 日志表
 *
 * | ip            | userId | eventTime | method | url          |
 * | ------------- | ------ | --------- | ------ | ------------ |
 * | 192.168.1.107 | 000001 |           | GET    | /project/get |
 */
public class PageViewCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment ste = StreamTableEnvironment.create(evn,environmentSettings);

        //获取数据
        ste.executeSql("create table view_log(\n" +
                "\tip string,\n" +
                "\tuserId string,\n" +
                "\teventTime bigint,\n" +
                "\t`method` string,\n" +
                "\turl string,\n" +
                "\tts as to_timestamp(from_unixtime(eventTime,'yyyy-MM-dd HH:mm:ss')),\n" +
                "\twatermark for ts as ts - interval '5' second\n" +
                ") with (\n" +
                "\t'connector'='jdbc',\n" +
                "\t'url'='jdbc:mysql://bigdata03:3306/mydb?useSSL=false',\n" +
                "\t'driver'='com.mysql.jdbc.Driver',\n" +
                "\t'table-name'='view_log',\n" +
                "\t'username'='root',\n" +
                "\t'password'='123456'\n" +
                ")");

        //排名前三的访问url
        Table t1 = ste.sqlQuery("select url, viewCount,w_start,rk \n" +
                "from (\n" +
                "\tselect *,row_number() over(partition by w_start order by viewCount desc) rk \n" +
                "\tfrom(\n" +
                "\t\tselect url,\n" +
                "\t\tcount(*) viewCount,\n" +
                "\t\thop_start(ts,interval '5' second,interval '10' minute) w_start\n" +
                "\t\tfrom view_log\n" +
                "\t\tgroup by hop(ts,interval '5' second,interval '10' minute),url\n" +
                "\t)\n" +
                ")\n" +
                "where rk<=3");
        ste.createTemporaryView("t1",t1);
        //sink数据
        ste.executeSql("create table sink_view(\n" +
                "\turl string,\n" +
                "\tviewCount bigint,\n" +
                "\tw_start string,\n" +
                "\trk int\n" +
                ") with (\n" +
                "\t'connector'='print'\n" +
                ")");

        t1.insertInto("sink_view");

    }
}
