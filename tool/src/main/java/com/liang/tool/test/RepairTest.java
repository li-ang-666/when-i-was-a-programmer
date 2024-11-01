package com.liang.tool.test;

import com.liang.tool.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) {
        BigDecimal bigDecimal = new BigDecimal("0.01%");
        bigDecimal.multiply(new BigDecimal("0.1"));
        System.out.println(bigDecimal.toPlainString());
    }
}
