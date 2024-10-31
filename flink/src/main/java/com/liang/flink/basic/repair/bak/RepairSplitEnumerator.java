package com.liang.flink.basic.repair.bak;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public class RepairSplitEnumerator {
    private static final int BATCH_SIZE = 10000;
    private static final int THREAD_NUM = 128;
    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);

    @SuppressWarnings("ConstantConditions")
    public Roaring64Bitmap getAllIds(RepairTask repairTask) throws Exception {
        long startTime = System.currentTimeMillis();
        Roaring64Bitmap allIds = new Roaring64Bitmap();
        // 查询边界, 初始化待查询分片队列
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        String sql = new SQL().SELECT("MIN(id)", "MAX(id)")
                .FROM(repairTask.getTableName())
                .toString();
        Queue<UncheckedSplit> uncheckedSplits = new ConcurrentLinkedQueue<>(splitUncheckedSplit(jdbcTemplate.queryForObject(sql, rs -> new UncheckedSplit(rs.getLong(1), rs.getLong(2))), THREAD_NUM));
        // 循环处理分片队列
        int times = 0;
        while (!uncheckedSplits.isEmpty()) {
            // 使用优先队列优先切分最大的分片, 尽可能补全分片数到线程数
            Queue<UncheckedSplit> priorityQueue = new PriorityQueue<>(uncheckedSplits);
            executorService.submit(uncheckedSplits::clear);
            while (priorityQueue.size() < THREAD_NUM) {
                UncheckedSplit maxIntervalSplit = priorityQueue.poll();
                // 最大分片也只能切为 1 片, 则不再切
                if (maxIntervalSplit.getR() - maxIntervalSplit.getL() + 1 <= BATCH_SIZE) {
                    priorityQueue.add(maxIntervalSplit);
                    break;
                } else {
                    priorityQueue.addAll(splitUncheckedSplit(maxIntervalSplit, THREAD_NUM - priorityQueue.size()));
                }
            }
            // 执行任务
            AtomicBoolean running = new AtomicBoolean(true);
            List<SplitTask> tasks = priorityQueue.parallelStream()
                    .map(split -> new SplitTask(uncheckedSplits, allIds, repairTask, split, running))
                    .collect(Collectors.toList());
            executorService.invokeAll(tasks);
            // 汇报log
            if (++times % 10 == 0) {
                log.info("times: {}, id num: {}", times, String.format("%,d", allIds.getLongCardinality()));
            }
        }
        executorService.shutdown();
        log.info("time: {} seconds, id num: {}", (System.currentTimeMillis() - startTime) / 1000, String.format("%,d", allIds.getLongCardinality()));
        return allIds;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private List<UncheckedSplit> splitUncheckedSplit(UncheckedSplit uncheckedSplit, int estimatedNum) {
        List<UncheckedSplit> result = new ArrayList<>(estimatedNum);
        long l = uncheckedSplit.getL();
        long r = uncheckedSplit.getR();
        // 无效边界
        if (l > r) {
        }
        // 不足或者正好 1 个
        else if (r - l + 1 <= BATCH_SIZE) {
            result.add(uncheckedSplit);
        }
        // 不足或者正好 estimatedNum 个
        else if (r - l + 1 <= (long) estimatedNum * BATCH_SIZE) {
            long interval = BATCH_SIZE - 1;
            while (l <= r) {
                result.add(new UncheckedSplit(l, Math.min(l + interval, r)));
                l = l + interval + 1;
            }
        }
        // 大分片, 稳定拆分为 estimatedNum 个
        else {
            long interval = ((r - l) / estimatedNum) + 1;
            while (l <= r) {
                result.add(new UncheckedSplit(l, Math.min(l + interval, r)));
                l = l + interval + 1;
            }
        }
        return result;
    }

    @RequiredArgsConstructor
    private static final class SplitTask implements Callable<Void> {
        private final Queue<UncheckedSplit> uncheckedSplits;
        private final Roaring64Bitmap allIds;
        private final RepairTask repairTask;
        private final UncheckedSplit uncheckedSplit;
        private final AtomicBoolean running;

        @Override
        public Void call() {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            long l = uncheckedSplit.getL();
            long r = uncheckedSplit.getR();
            while (true) {
                List<Long> ids;
                if (l > r) {
                    ids = new ArrayList<>();
                } else {
                    String sql = new SQL().SELECT("id")
                            .FROM(repairTask.getTableName())
                            .WHERE("id >= " + l)
                            .WHERE("id <= " + r)
                            .ORDER_BY("id ASC")
                            .LIMIT(BATCH_SIZE)
                            .toString();
                    ids = jdbcTemplate.queryForList(sql, rs -> rs.getLong(1));
                }
                // 如果本线程 [自然] 执行完毕
                if (ids.isEmpty()) {
                    running.set(false);
                    return null;
                }
                // 收集本批次id, 准备寻找下批次id
                synchronized (allIds) {
                    allIds.add(ids.parallelStream().mapToLong(e -> e).toArray());
                }
                l = ids.get(ids.size() - 1) + 1;
                // 如果本线程 [被动] 执行完毕
                if (!running.get()) {
                    // 返还未处理分片
                    if (l <= r) {
                        uncheckedSplits.add(new UncheckedSplit(l, r));
                    }
                    return null;
                }
            }
        }
    }

    @Data
    @AllArgsConstructor
    private static final class UncheckedSplit implements Comparable<UncheckedSplit> {
        private long l;
        private long r;

        @Override
        public int compareTo(UncheckedSplit another) {
            long diff = this.r - this.l;
            long anotherDiff = another.r - another.l;
            if (anotherDiff - diff > 0) {
                return 1;
            } else if (anotherDiff - diff < 0) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
