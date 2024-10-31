package com.liang.flink.job;

import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.service.LocalConfigFile;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@LocalConfigFile("group.yml")
public class GroupJob {
    public static void main(String[] args) {
        EnvironmentFactory.create(args);
        Map<Node, Path> result = new LinkedHashMap<>();
        JdbcTemplate graphData430 = new JdbcTemplate("430.graph_data");
        String groupName = "上海宝信软件股份有限公司";
        Queue<Path> queue = new ArrayDeque<Path>() {{
            add(Path.newPath(new Node("27624827", "上海宝信软件股份有限公司")));
        }};
        queue.forEach(root -> result.put((Node) root.elements.get(0), root));
        while (!queue.isEmpty()) {
            int size = queue.size();
            while (size-- > 0) {
                Path polledPath = queue.poll();
                String polledShareholderId = polledPath.getLast().getId();
                String sql = new SQL().SELECT("*")
                        .FROM("company_equity_relation_details")
                        .WHERE("shareholder_id = " + SqlUtils.formatValue(polledShareholderId))
                        .ORDER_BY("equity_ratio desc")
                        .toString();
                List<Map<String, Object>> columnMaps = graphData430.queryForColumnMaps(sql);
                for (Map<String, Object> columnMap : columnMaps) {
                    String investedCompanyId = (String) columnMap.get("company_id");
                    String investedCompanyName = (String) columnMap.get("company_name");
                    String equityRatio = (String) columnMap.get("equity_ratio");
                    String maxEquityRatioSql = new SQL().SELECT("max(equity_ratio)")
                            .FROM("company_equity_relation_details")
                            .WHERE("company_id = " + SqlUtils.formatValue(investedCompanyId))
                            .toString();
                    String maxRatio = graphData430.queryForObject(maxEquityRatioSql, rs -> rs.getString(1));
                    if (maxRatio.equals(equityRatio)) {
                        Edge edge = new Edge(new BigDecimal(equityRatio).toPlainString());
                        Node node = new Node(investedCompanyId, investedCompanyName);
                        Path path = Path.newPath(polledPath, edge, node);
                        if (!result.containsKey(node)) {
                            result.put(node, path);
                            queue.add(path);
                        }
                    }
                }
            }
        }
        CsvWriter writer = CsvUtil.getWriter(new File("/Users/liang/Desktop/" + groupName + ".csv"), StandardCharsets.UTF_8);
        writer.writeHeaderLine("company_id", "company_name", "level", "reason", "info");
        result.forEach((k, v) -> {
            String companyId = k.getId();
            String companyName = k.getName();
            String level = String.valueOf(v.getLevel());
            String reason = "每一跳 都是 该公司所有股东 最大股比 (非唯一, 比如两个50%, 或者3个30% + 1个10%)";
            String info = v.toJsonString();
            writer.writeLine(companyId, companyName, level, reason, info);
        });
        writer.flush();
    }

    private interface Element {
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static final class Node implements Element {
        String id;
        String name;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static final class Edge implements Element {
        String info;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static final class Path {
        private List<Element> elements = new ArrayList<>();

        public static Path newPath(Node node) {
            Path path = new Path();
            path.elements.add(node);
            return path;
        }

        public static Path newPath(Path path, Edge edge, Node node) {
            Path newPath = new Path();
            newPath.elements.addAll(path.elements);
            newPath.elements.add(edge);
            newPath.elements.add(node);
            return newPath;
        }

        public Node getLast() {
            return (Node) elements.get(elements.size() - 1);
        }

        public String toJsonString() {
            return JsonUtils.toString(elements.stream().map(element -> {
                        if (element instanceof Edge) {
                            return ((Edge) element).info;
                        } else {
                            return element;
                        }
                    })
                    .collect(Collectors.toList()));
        }

        public int getLevel() {
            return (elements.size() - 1) / 2;
        }
    }
}
