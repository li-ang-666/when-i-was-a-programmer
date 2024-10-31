package com.liang.flink.project.group.bfs;

import cn.hutool.core.collection.CollUtil;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Slf4j
public class GroupBfsService {
    private static final BigDecimal THRESHOLD = new BigDecimal("0.0001").setScale(12, RoundingMode.DOWN);
    private final GroupBfsDao dao = new GroupBfsDao();
    private final Queue<Path> bfsQueue = new ConcurrentLinkedQueue<>();
    private final Map<String, Set<Path>> result = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig();
        ConfigUtils.setConfig(config);
        new GroupBfsService().bfs("25942218");
    }

    public void bfs(String biggestShareholderId) {
        // 格式合法
        if (!TycUtils.isUnsignedId(biggestShareholderId)) return;
        // 没有股东
        if (dao.queryHasShareholder(biggestShareholderId)) return;
        // bfs
        int level = 0;
        bfsQueue.add(Path.of(new Node(biggestShareholderId)));
        while (!bfsQueue.isEmpty()) {
            log.info("level: {}, size: {}", level++, bfsQueue.size());
            // 切分为多段
            List<List<Path>> subLists = CollUtil.split(bfsQueue, 10_000);
            bfsQueue.clear();
            for (List<Path> subList : subLists) {
                // 查询该段所有股东的对外投资数据
                Map<String, Set<Tuple2<Edge, Node>>> investInfos = dao.queryInvestInfos(subList.parallelStream().map(e -> e.getLastNode().getId()).collect(Collectors.toList()));
                // 多线程处理
                subList.parallelStream().forEach(path -> {
                    Node lastNode = path.getLastNode();
                    String lastId = lastNode.getId();
                    // 记录该path
                    result.compute(lastId, (k, v) -> {
                        v = ObjUtil.defaultIfNull(v, ConcurrentHashMap.newKeySet());
                        v.add(path);
                        return v;
                    });
                    // 若存在对外投资, 且不满足终止遍历的条件, 产出new path
                    if (investInfos.containsKey(lastId) && path.canContinueBfs()) {
                        investInfos.get(lastId).parallelStream().forEach(edgeAndNode -> {
                            Path newPath = Path.of(path, edgeAndNode.f0, edgeAndNode.f1);
                            // 不满足终止遍历条件的new path, 才加入queue
                            if (newPath.canContinueBfs()) {
                                bfsQueue.add(newPath);
                            }
                        });
                    }
                });
            }
        }
        log.info("size: {}", result.size());
    }

    public interface pathElement extends Serializable {
    }

    @Data
    public static class Node implements pathElement {
        private final String id;
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
