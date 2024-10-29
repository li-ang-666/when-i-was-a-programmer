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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class GroupBfsService {
    private static final BigDecimal THRESHOLD = new BigDecimal("0.0001");
    private final GroupBfsDao dao = new GroupBfsDao();
    private final Queue<Path> bfsPaths = new ArrayDeque<>();
    private final Map<Node, List<Path>> result = new HashMap<>();
    private final Map<String, List<Tuple2<Edge, Node>>> cachedInvestInfo = new HashMap<>();
    private int level = 0;

    public static void main(String[] args) throws Exception {
        Config config = ConfigUtils.createConfig();
        ConfigUtils.setConfig(config);
        new GroupBfsService().bfs("25942218");
    }

    public void bfs(String companyId) throws Exception {
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
            dao.cacheInvested(bfsPaths.stream().map(path -> path.getLastNode().getId()).collect(Collectors.toList()), cachedInvestInfo);
            int size = bfsPaths.size();
            {
                log.info("level: {}, size: {}", level++, size);
                TimeUnit.SECONDS.sleep(1);
            }
            while (size-- > 0) {
                Path polledPath = bfsPaths.poll();
                Node polledPathLastNode = polledPath.getLastNode();
                String polledPathLastId = polledPathLastNode.getId();
                for (Tuple2<Edge, Node> edgeAndNode : cachedInvestInfo.get(polledPathLastId)) {
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
        private BigDecimal totalRatio;
        private Node lastNode;
        private List<String> ids = new ArrayList<>();
        private Set<String> distinctIds = new HashSet<>();

        public static Path of(Node node) {
            Path path = new Path();
            path.getPathElements().add(node);
            // 边
            path.setTotalRatio(new BigDecimal("1"));
            // 点
            path.setLastNode(node);
            path.getIds().add(node.getId());
            path.getDistinctIds().add(node.getId());
            return path;
        }

        public static Path of(Path oldPath, Edge edge, Node node) {
            Path path = ObjUtil.cloneByStream(oldPath);
            path.getPathElements().add(edge);
            path.getPathElements().add(node);
            // 边
            path.setTotalRatio(path.getTotalRatio().multiply(edge.getRatio()));
            // 点
            path.setLastNode(node);
            path.getIds().add(node.getId());
            path.getDistinctIds().add(node.getId());
            return path;
        }

        public boolean canContinueBfs() {
            boolean biggerThanThreshold = getTotalRatio().compareTo(THRESHOLD) >= 0;
            boolean noCircle = getIds().size() == getDistinctIds().size();
            return biggerThanThreshold && noCircle;
        }
    }
}
