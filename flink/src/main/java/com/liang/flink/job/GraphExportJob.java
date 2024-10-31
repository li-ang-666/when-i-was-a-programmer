package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.connector.storage.obs.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.graph.export.GraphExportDao;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

//  -- beeline

//  create table if not exists test.graph_export_edge(
//    `row` string
//  )stored as textfile location 'obs://hadoop-obs/flink/graph/edge';

//  create table if not exists test.graph_export_node(
//    `row` string
//  )stored as textfile location 'obs://hadoop-obs/flink/graph/node';

//  -- 建表后再写入数据
//  select count(1) from test.graph_export_edge;
//  select count(1) from test.graph_export_node;

//  -- spark-sql
//  insert overwrite table test.graph_export_edge select /*+ REPARTITION(6) */ * from test.graph_export_edge;
//  insert overwrite table test.graph_export_node select /*+ REPARTITION(12) */ distinct * from test.graph_export_node;
@LocalConfigFile("graph-export.yml")
public class GraphExportJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new GraphExportSink(config))
                .name("GraphExportSink")
                .uid("GraphExportSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("GraphExportJob");
    }

    private static String format(Object obj) {
        return ((String) obj).replaceAll("[\\s\n,\r'\"]", "");
    }

    @RequiredArgsConstructor
    private final static class GraphExportSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private ObsWriter edgeObsWriter;
        private ObsWriter nodeObsWriter;
        private GraphExportDao dao;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            edgeObsWriter = new ObsWriter("obs://hadoop-obs/flink/graph/edge", ObsWriter.FileFormat.CSV);
            edgeObsWriter.enableCache();
            nodeObsWriter = new ObsWriter("obs://hadoop-obs/flink/graph/node", ObsWriter.FileFormat.CSV);
            nodeObsWriter.enableCache();
            dao = new GraphExportDao();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            Tuple2<String, String> node = getNode(columnMap);
            if (node == null) {
                return;
            }
            nodeObsWriter.update(node.f0);
            nodeObsWriter.update(node.f1);
            edgeObsWriter.update(getEdge(columnMap));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            edgeObsWriter.flush();
            nodeObsWriter.flush();
        }

        @Override
        public void finish() {
            edgeObsWriter.flush();
            nodeObsWriter.flush();
        }

        @Override
        public void close() {
            edgeObsWriter.flush();
            nodeObsWriter.flush();
        }

        private String getEdge(Map<String, Object> columnMap) {
            List<String> edge = Arrays.asList(
                    format(columnMap.get("tyc_unique_entity_id_investor")),
                    format(columnMap.get("company_id_invested")),
                    "equity_relation",
                    format(columnMap.get("equity_ratio")),
                    format(columnMap.get("equity_amount")),
                    format(columnMap.get("equity_amount_currency")),
                    format(columnMap.get("reference_pt_year")),
                    "1704038400000"
            );
            return String.join(",", edge);
        }

        private Tuple2<String, String> getNode(Map<String, Object> columnMap) {
            // 被投资公司
            List<String> company = Arrays.asList(
                    format(columnMap.get("company_id_invested")),
                    "node",
                    "2",
                    format(columnMap.get("company_id_invested")),
                    "0",
                    format(columnMap.get("tyc_unique_entity_name_invested")),
                    "",
                    String.valueOf(!dao.isClosed((String) columnMap.get("company_id_invested"))),
                    "1704038400000"
            );
            String type = String.valueOf(columnMap.get("investor_identity_type"));
            List<String> shareholder;
            // 股东-公司
            if ("2".equals(type)) {
                shareholder = Arrays.asList(
                        format(columnMap.get("company_id_investor")),
                        "node",
                        "2",
                        format(columnMap.get("company_id_investor")),
                        "0",
                        format(columnMap.get("tyc_unique_entity_name_investor")),
                        "",
                        String.valueOf(!dao.isClosed((String) columnMap.get("company_id_investor"))),
                        "1704038400000"
                );
            }
            // 股东-人
            else if ("1".equals(type)) {
                shareholder = Arrays.asList(
                        format(columnMap.get("tyc_unique_entity_id_investor")),
                        "node",
                        "1",
                        format(columnMap.get("company_id_invested")),
                        format(columnMap.get("company_id_investor")),
                        format(columnMap.get("tyc_unique_entity_name_investor")),
                        "",
                        "true",
                        "1704038400000"
                );
            } else {
                return null;
            }
            return Tuple2.of(String.join(",", company), String.join(",", shareholder));
        }
    }
}
