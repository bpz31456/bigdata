package cn.lfar.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class LoginFailWarning {
    private String userId;
    private Long firstFailTime;
    private Long lastFailTime;
    private String warningMsg;
}
