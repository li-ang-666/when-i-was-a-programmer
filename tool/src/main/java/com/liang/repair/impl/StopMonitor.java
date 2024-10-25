package com.liang.repair.impl;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import com.liang.common.util.JsonUtils;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StopMonitor {
    private final static String URL = "http://10.99.205.87:8990/flink/cancelMonitor";
    private final static List<Tuple3<String, String, String>> LIST = new ArrayList<>();

    static {
        LIST.add(Tuple3.of("liang", "Moka20190520", "InvestmentRelationJob"));
        LIST.add(Tuple3.of("liang", "Moka20190520", "company_bid_parsed_info"));
        LIST.add(Tuple3.of("liang", "Moka20190520", "PatentJob"));
        LIST.add(Tuple3.of("liang", "Moka20190520", "BidAiV1Job"));
        LIST.add(Tuple3.of("liang", "Moka20190520", "BidAiV2Job"));
        LIST.add(Tuple3.of("liang", "Moka20190520", "BidToCloudJob火山云线上任务"));
        LIST.add(Tuple3.of("liang", "Moka20190520", "company_bid_parsed_info"));
        LIST.add(Tuple3.of("liang", "Moka20190520", "BidJob离线修复"));
        LIST.add(Tuple3.of("liang", "Moka20190520", "RelationEdgeJob"));
        LIST.add(Tuple3.of("liang", "Moka20190520", "DemoJob"));
    }

    public static void main(String[] args) {
        HttpRequest post = HttpUtil.createPost(URL);
        LIST.forEach(info -> {
            Map<String, Object> map = new HashMap<>();
            map.put("sshUserName", info.f0);
            map.put("sshPassWord", info.f1);
            map.put("yarnName", info.f2);
            map.put("isMonitored", 0);
            HttpResponse response = post.body(JsonUtils.toString(map), "application/json").execute();
            System.out.println(response.body());
        });
    }
}
