package com.liang.flink.project.group.bfs;

import cn.hutool.core.util.ObjUtil;
import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

@Slf4j
public class GroupBfsService {
    private final GroupBfsDao dao = new GroupBfsDao();
    private final Queue<Path> bfsPaths = new ArrayDeque<>();
    private final Map<Node, List<Path>> result = new HashMap<>();
    private int level = 0;

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig();
        ConfigUtils.setConfig(config);
        new GroupBfsService().bfs("25942218");
    }

    public void bfs(String companyId) {
        // 格式合法
        if (!TycUtils.isUnsignedId(companyId)) {
            return;
        }
        // 在company_index存在
        Map<String, Object> companyIndexColumnMap = dao.queryCompanyIndex(companyId);
        if (companyIndexColumnMap.isEmpty()) {
            return;
        }
        // 没有股东
        if (dao.queryHasShareholder(companyId)) {
            return;
        }
        String companyName = (String) companyIndexColumnMap.get("company_name");
        Node rootNode = new Node(companyId, companyName);
        Path rootPath = Path.of(rootNode);

        bfsPaths.add(rootPath);

        while (!bfsPaths.isEmpty()) {
            level++;
            int size = bfsPaths.size();
            while (size-- > 0) {
                Path polledPath = bfsPaths.poll();
                Node polledPathLastNode = polledPath.getLastNode();
                String polledPathLastId = polledPathLastNode.getId();
                List<Tuple2<Edge, Node>> edgeAndNodes = dao.queryInvested(polledPathLastId);
                for (Tuple2<Edge, Node> edgeAndNode : edgeAndNodes) {
                    Path newPath = Path.of(polledPath, edgeAndNode.f0, edgeAndNode.f1);
                    if (newPath.canContinueBfs()) {
                        bfsPaths.add(newPath);
                    } else {
                        result.compute(polledPathLastNode, (k, v) -> {
                            List<Path> paths = v != null ? v : new ArrayList<>();
                            paths.add(newPath);
                            return paths;
                        });
                    }
                }
            }
        }
        result.forEach((k, v) -> {
            System.out.println(k + " -> " + v);
        });
    }

    public interface pathElement extends Serializable {
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Node implements pathElement {
        private String id;
        private String name;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Edge implements pathElement {
        private BigDecimal ratio;
    }

    @Data
    public static class Path implements Serializable {
        private List<pathElement> pathElements = new ArrayList<>();

        public static Path of(Node node) {
            Path path = new Path();
            path.getPathElements().add(node);
            return path;
        }

        public static Path of(Path oldPath, Edge edge, Node node) {
            Path path = ObjUtil.cloneByStream(oldPath);
            path.getPathElements().add(edge);
            path.getPathElements().add(node);
            return path;
        }

        public Node getLastNode() {
            return (Node) pathElements.get(pathElements.size() - 1);
        }

        public BigDecimal getRatio() {
            BigDecimal bigDecimal = new BigDecimal("1");
            pathElements.stream()
                    .filter(e -> e instanceof Edge)
                    .map(e -> ((Edge) e).getRatio())
                    .forEach(bigDecimal::multiply);
            return bigDecimal;
        }

        public boolean canContinueBfs() {
            return this.getRatio().compareTo(BigDecimal.ZERO) != 0;
        }
    }
}
