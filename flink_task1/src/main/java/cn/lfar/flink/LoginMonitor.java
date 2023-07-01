package cn.lfar.flink;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * 需求三：恶意登录监控
 * 用户在短时间内频繁登录失败，有程序恶意攻击的可能，同一用户（可以是不同IP）在2秒内连续两次登录失败，需要报警。
 */
public class LoginMonitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<LoginEvent, String> loginEventStringKeyedStream = env.readTextFile("file:///home/bpz/workspace/projects/github/bigdata/flink_task1/src/main/resources/login.log")
                .map(line -> {
                    String[] logEventInfo = line.split(",");
                    return new LoginEvent(logEventInfo[0], Long.parseLong(logEventInfo[1]), logEventInfo[2], logEventInfo[3]);
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getLoginTime();
                    }
                }).keyBy(new KeySelector<LoginEvent, String>() {
                    @Override
                    public String getKey(LoginEvent value) throws Exception {
                        return value.getUserId();
                    }
                });

        Pattern<LoginEvent, LoginEvent> failEvents = Pattern.<LoginEvent>begin("loginFailEvents").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getState());
            }
        }).times(2).consecutive().within(Time.seconds(2));

        PatternStream<LoginEvent> pattern = CEP.pattern(loginEventStringKeyedStream, failEvents);

        pattern.select(new PatternSelectFunction<LoginEvent, LoginFailWarning>() {
            @Override
            public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent firstEvent = map.get("loginFailEvents").get(0);
                LoginEvent secEvent = map.get("loginFailEvents").get(1);
                return new LoginFailWarning(firstEvent.getUserId(),
                        firstEvent.getLoginTime(),
                        secEvent.getLoginTime(),
                        "登陆异常，连续登陆"+map.get("loginFailEvents").size()+"次错误。" );
            }
        }).print();
        env.execute("登陆异常检测 cep 任务");
    }
}


