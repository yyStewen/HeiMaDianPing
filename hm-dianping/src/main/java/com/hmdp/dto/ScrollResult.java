package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {//滚动分页
    private List<?> list;
    private Long minTime;
    private Integer offset;
}
