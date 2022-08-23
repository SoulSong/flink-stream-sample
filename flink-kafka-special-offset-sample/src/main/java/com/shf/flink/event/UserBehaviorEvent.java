package com.shf.flink.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class UserBehaviorEvent {
    private Integer userId;
    private Integer itemId;
    private String category;
    private String clientIp;
    private String action;
    private Long ts;
}
