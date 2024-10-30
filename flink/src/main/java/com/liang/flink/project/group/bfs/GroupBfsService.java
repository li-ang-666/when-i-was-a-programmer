package com.liang.flink.project.group.bfs;

import cn.hutool.core.util.ObjUtil;
import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GroupBfsService {
    private static final BigDecimal THRESHOLD = new BigDecimal("0.0001").setScale(12, RoundingMode.DOWN);
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
            int size = bfsPaths.size();
            {
                log.info("level: {}, size: {}", level++, size);
                TimeUnit.SECONDS.sleep(1);
            }
            while (size-- > 0) {
                Path polledPath = bfsPaths.remove();
                Node polledPathLastNode = polledPath.getLastNode();
                String polledPathLastId = polledPathLastNode.getId();
                dao.cacheInvested(Collections.singleton(polledPathLastId), cachedInvestInfo);
                List<Tuple2<Edge, Node>> investInfo = cachedInvestInfo.get(polledPathLastId);
                // 如果没有后续对外投资
                if (investInfo == null) {
                    result.compute(polledPathLastNode, (k, v) -> {
                        List<Path> paths = ObjUtil.defaultIfNull(v, new ArrayList<>());
                        paths.add(polledPath);
                        return paths;
                    });
                }
                // 如果仍有后续对外投资
                else {
                    for (Tuple2<Edge, Node> edgeAndNode : investInfo) {
                        Path newPath = Path.of(polledPath, edgeAndNode.f0, edgeAndNode.f1);
                        if (newPath.canContinueBfs()) {
                            bfsPaths.add(newPath);
                        } else {
                            result.compute(polledPathLastNode, (k, v) -> {
                                List<Path> paths = ObjUtil.defaultIfNull(v, new ArrayList<>());
                                paths.add(newPath);
                                return paths;
                            });
                        }
                    }
                }
            }
        }
//        result.forEach((k, v) -> System.out.println(k + " -> " + v));
    }

    public interface pathElement extends Serializable {
    }

    @Data
    public static class Node implements pathElement {
        private final String id;
        private final String name;
    }

    @Data
    public static class Edge implements pathElement {
        private final BigDecimal ratio;
    }

    @Data
    public static class Path implements Serializable {
        private final List<pathElement> pathElements = new ArrayList<>();
        private final List<String> ids = new ArrayList<>();
        private final Set<String> distinctIds = new HashSet<>();
        private BigDecimal totalRatio;
        private Node lastNode;

        public static Path of(Node node) {
            Path path = new Path();
            // pathElements
            path.getPathElements().add(node);
            // ids
            path.getIds().add(node.getId());
            // distinctIds
            path.getDistinctIds().add(node.getId());
            // totalRatio
            path.setTotalRatio(new BigDecimal("1").setScale(12, RoundingMode.DOWN));
            // lastNode
            path.setLastNode(node);
            return path;
        }

        public static Path of(Path oldPath, Edge edge, Node node) {
            Path path = new Path();
            // pathElements
            path.getPathElements().addAll(oldPath.getPathElements());
            path.getPathElements().add(edge);
            path.getPathElements().add(node);
            // ids
            path.getIds().addAll(oldPath.getIds());
            path.getIds().add(node.getId());
            // distinctIds
            path.getDistinctIds().addAll(oldPath.getDistinctIds());
            path.getDistinctIds().add(node.getId());
            // totalRatio
            path.setTotalRatio(oldPath.getTotalRatio().multiply(edge.getRatio()).setScale(12, RoundingMode.DOWN));
            // lastNode
            path.setLastNode(node);
            return path;
        }

        public boolean canContinueBfs() {
            boolean biggerThanThreshold = getTotalRatio().compareTo(THRESHOLD) >= 0;
            boolean noCircle = getIds().size() == getDistinctIds().size();
            return biggerThanThreshold && noCircle;
        }
    }
}
