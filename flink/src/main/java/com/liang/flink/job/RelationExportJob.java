package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.service.connector.storage.obs.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Map;

@Slf4j
@LocalConfigFile("relation-export.yml")
public class RelationExportJob {
    static {
        RelationExportJob.class.getClassLoader().getResource("relation-export.sql");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .addSink(new RelationExportSink(config))
                .name("RelationExportSink")
                .uid("RelationExportSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("RelationExportJob");
    }

    @RequiredArgsConstructor
    private static final class RelationExportSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private final Roaring64Bitmap emptyCompany = new Roaring64Bitmap();
        private ObsWriter companyObsWriter;
        private ObsWriter humanObsWriter;
        private ObsWriter edgeObsWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            companyObsWriter = new ObsWriter("obs://hadoop-obs/flink/relation/node/company/", ObsWriter.FileFormat.CSV);
            companyObsWriter.enableCache();
            humanObsWriter = new ObsWriter("obs://hadoop-obs/flink/relation/node/human/", ObsWriter.FileFormat.CSV);
            humanObsWriter.enableCache();
            edgeObsWriter = new ObsWriter("obs://hadoop-obs/flink/relation/edge/", ObsWriter.FileFormat.CSV);
            edgeObsWriter.enableCache();
            new JdbcTemplate("116.prism")
                    .streamQuery(true,
                            "select graph_id from entity_empty_index where entity_type = 2 and deleted = 0",
                            rs -> emptyCompany.add(rs.getLong(1)));
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            String table = singleCanalBinlog.getTable();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String regexp = "[\"',\\s]";
            if ("company_index".equals(table)) {
                long companyId = Long.parseLong((String) columnMap.get("company_id"));
                String odsStatus = (String) columnMap.get("company_registation_status");
                int status;
                if (odsStatus.contains("注销") && !odsStatus.contains("未注销")) {
                    status = 1;
                } else if (odsStatus.contains("吊销")) {
                    status = 2;
                } else {
                    status = 0;
                }
                boolean isEmpty = emptyCompany.contains(companyId);
                String companyName = ((String) columnMap.get("company_name")).replaceAll(regexp, "");
                companyObsWriter.update(String.join(",", String.valueOf(companyId), "company", String.valueOf(status), String.valueOf(isEmpty), companyName));
            } else if ("human".equals(table)) {
                String pid = (String) columnMap.get("human_id");
                String gid = (String) columnMap.get("human_name_id");
                String name = ((String) columnMap.get("human_name")).replaceAll(regexp, "");
                if (TycUtils.isTycUniqueEntityId(pid) && TycUtils.isUnsignedId(gid)) {
                    humanObsWriter.update(String.join(",", pid, "human", gid, name));
                }
            } else if ("relation_edge".equals(table)) {
                String sourceId = (String) columnMap.get("source_id");
                String targetId = (String) columnMap.get("target_id");
                String relation = (String) columnMap.get("relation");
                String other = ((String) columnMap.get("other")).replaceAll(regexp, "");
                edgeObsWriter.update(String.join(",", sourceId, targetId, relation, other));
            } else if ("personnel_merge".equals(table)) {
                String sourceId = (String) columnMap.get("human_id");
                String targetId = (String) columnMap.get("company_id");
                String isHistory = (String) columnMap.get("is_history");
                String relation = "1".equals(isHistory) ? "HIS_SERVE" : "SERVE";
                String other = ((String) columnMap.get("personnel_position"))
                        .replaceAll(",", "，")
                        .replaceAll(regexp, "");
                if (TycUtils.isTycUniqueEntityId(sourceId) && TycUtils.isUnsignedId(targetId)) {
                    edgeObsWriter.update(String.join(",", sourceId, targetId, relation, other));
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            companyObsWriter.flush();
            humanObsWriter.flush();
            edgeObsWriter.flush();
        }

        @Override
        public void close() {
            companyObsWriter.flush();
            humanObsWriter.flush();
            edgeObsWriter.flush();
        }

        @Override
        public void finish() {
            companyObsWriter.flush();
            humanObsWriter.flush();
            edgeObsWriter.flush();
        }
    }
}
