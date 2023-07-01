package cn.lfar.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * ## 需求四：订单支付实时监控
 *
 * 最终创造收入和利润的是用户下单购买的环节；更具体一点，是用户真正完成支付动作的时候。用户下单的行为可以表明用户对商品的需求，但在现实中，并不是每次下单都会被用户立刻支付。
 * 当拖延一段时间后，用户支付的意愿会降低，并且为了降低安全风险，电商网站往往会对订单状态进行监控。设置一个失效时间（比如15分钟），如果下单后一段时间仍未支付，订单就会被取消。
 */
public class OrderTimeOut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<OrderEvent, String> dataSource = env.fromElements(
                new OrderEvent("000001", "create", 1688189260 * 1000L),
                new OrderEvent("000001", "pay", 1688199261 * 1000L),
                new OrderEvent("000002", "create", 1688189262 * 1000L),
                new OrderEvent("000002", "pay", 1688189363 * 1000L),
                new OrderEvent("000003", "create", 1688189264 * 1000L)
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(OrderEvent element) {
                return element.getTimestamp();
            }
        }).keyBy(new KeySelector<OrderEvent, String>() {
            @Override
            public String getKey(OrderEvent value) throws Exception {
                return value.getOrderId();
            }
        });

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("orderTimeout").where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                        return orderEvent.getActive().equals("create");
                    }
                }).followedBy("pay")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {

                        return orderEvent.getActive().equals("pay");
                    }
                }).within(Time.seconds(600));

        OutputTag<OrderEvent> orderTimeoutTag = new OutputTag<OrderEvent>("orderTimeout"){};
        DataStream<OrderEvent> sideOutput = CEP.pattern(dataSource, pattern).select(orderTimeoutTag, new PatternTimeoutFunction<OrderEvent, OrderEvent>() {
            @Override
            public OrderEvent timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                return map.get("orderTimeout").get(0);
            }
        }, new PatternSelectFunction<OrderEvent, OrderEvent>() {
            @Override
            public OrderEvent select(Map<String, List<OrderEvent>> map) throws Exception {
                return map.get("pay").get(0);
            }
        }).getSideOutput(orderTimeoutTag);

        sideOutput.print();
        env.execute("订单超时 cep检测");

    }
}
