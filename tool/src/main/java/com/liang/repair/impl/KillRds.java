package com.liang.repair.impl;

import cn.hutool.core.text.csv.CsvReadConfig;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.http.HttpUtil;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.shaded.guava31.com.google.common.net.HttpHeaders.*;

@SuppressWarnings("unchecked")
public class KillRds extends ConfigHolder {
    private static final Map<String, Object> IP_MAPPING;

    static {
        String body = HttpUtil.createPost("https://dbok.jindidata.com/system/ok/list")
                .header(ACCEPT, "application/json, text/javascript, */*; q=0.01")
                .header(ACCEPT_ENCODING, "gzip, deflate, br, zstd")
                .header(ACCEPT_LANGUAGE, "zh-CN,zh;q=0.9")
                .header(CONNECTION, "keep-alive")
                .header(CONTENT_LENGTH, "35")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(COOKIE, "sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22188346cad71d53-0bf63cd26b2d05-1c525634-1484784-188346cad72a88%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTg4MzQ2Y2FkNzFkNTMtMGJmNjNjZDI2YjJkMDUtMWM1MjU2MzQtMTQ4NDc4NC0xODgzNDZjYWQ3MmE4OCJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%22188346cad71d53-0bf63cd26b2d05-1c525634-1484784-188346cad72a88%22%7D; sso_jddt=Onm2DBgHZcjCNPwWe76D/LRNSBu2nL0O2UIFVQokRfZsG8t9xw2MWKjYbwkRxV1u; rememberMe=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; JSESSIONID=e97c22a9-22d4-4cfb-b447-e5c6b473a62f")
                .header(HOST, "dbok.jindidata.com")
                .header(ORIGIN, "https://dbok.jindidata.com")
                .header(REFERER, "https://dbok.jindidata.com/system/ok")
                .header(SEC_CH_UA, "\"Chromium\";v=\"128\", \"Not;A=Brand\";v=\"24\", \"Google Chrome\";v=\"128\"")
                .header(SEC_CH_UA_MOBILE, "?0")
                .header(SEC_CH_UA_PLATFORM, "\"macOS\"")
                .header(SEC_FETCH_DEST, "empty")
                .header(SEC_FETCH_MODE, "cors")
                .header(SEC_FETCH_SITE, "same-origin")
                .header(USER_AGENT, "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
                .header(X_REQUESTED_WITH, "XMLHttpRequest")
                .form("pageSize", "20000")
                .form("pageNum", "1")
                .form("isAsc", "asc")
                .form("s1", "")
                .execute()
                .body();
        IP_MAPPING = ((List<Map<String, Object>>) (JsonUtils.parseJsonObj(body).get("rows")))
                .stream()
                .filter(row -> "Server".equals(row.get("dbType")) && String.valueOf(row.get("connectionString")).matches("\\d+\\.\\d+\\.\\d+\\.\\d+.*"))
                .map(row -> new HashMap<String, Object>() {{
                    put("id", row.get("dbInstanceId"));
                    put("name", row.get("dbInstanceDescription"));
                    put("ip", row.get("connectionString"));
                }})
                .collect(Collectors.toMap(e -> (String) e.get("ip"), e -> e));
    }

    public static void main(String[] args) {
        File csvFile = new File("/Users/liang/Desktop/rds241.csv");
        CsvReadConfig csvReadConfig = CsvReadConfig.defaultConfig()
                .setHeaderLineNo(1)
                .setContainsHeader(true);
        Set<String> ips = new HashSet<>();
        for (CsvRow row : CsvUtil.getReader(csvReadConfig).read(csvFile)) {
            Map<String, String> rowMap = row.getFieldMap();
            ips.add(rowMap.get("clientIp"));
        }
        System.out.println("\n\n\n");
        for (String ip : ips) {
            if (IP_MAPPING.containsKey(ip)) {
                System.out.println(ip + " -> " + JsonUtils.toString(IP_MAPPING.get(ip)));
            } else {
                System.out.println("unknown ip: " + ip);
            }
        }
    }
}
