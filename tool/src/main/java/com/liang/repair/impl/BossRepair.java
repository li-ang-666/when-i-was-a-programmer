package com.liang.repair.impl;

import cn.hutool.http.HttpUtil;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class BossRepair extends ConfigHolder {
    private static final int TIMEOUT = (int) TimeUnit.HOURS.toMillis(24);

    public static void main(String[] args) {
        String body = HttpUtil.createPost("http://10.99.138.255:9060/update_boss")
                .form("hgid", "2002633907")
                .timeout(TIMEOUT)
                .execute()
                .body();
        log.info(body);
    }
}
