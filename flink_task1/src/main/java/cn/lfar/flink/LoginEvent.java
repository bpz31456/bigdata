package cn.lfar.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class LoginEvent {
    private String userId;
    private long loginTime;
    private String state;
    private String ip;
}
