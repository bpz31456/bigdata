package cn.lfar.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class OrderEvent {
    private String orderId;
    private String active;
    private Long timestamp;
}
