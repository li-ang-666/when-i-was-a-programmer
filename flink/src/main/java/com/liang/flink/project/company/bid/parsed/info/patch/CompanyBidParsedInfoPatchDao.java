package com.liang.flink.project.company.bid.parsed.info.patch;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CompanyBidParsedInfoPatchDao {
    private final static String SINK_TABLE = "operating_info.company_bid_parsed_info_patch";

    private final JdbcTemplate source = new JdbcTemplate("104.data_bid");
    private final JdbcTemplate companyBase = new JdbcTemplate("435.company_base");
    private final JdbcTemplate sink = new JdbcTemplate("448.operating_info");
    private final JdbcTemplate gauss = new JdbcTemplate("gauss");

    {
        sink.enableCache();
    }

    public String queryContent(String mainId) {
        String sql = new SQL().SELECT("content")
                .FROM("company_bid")
                .WHERE("id = " + SqlUtils.formatValue(mainId))
                .toString();
        String res = source.queryForObject(sql, rs -> rs.getString(1));
        return res != null ? res : "";
    }

    public String getCompanyIdByName(Object companyName) {
        if (!TycUtils.isValidName(companyName)) {
            return "";
        }
        String sql = new SQL().SELECT("company_id")
                .FROM("company_index")
                .WHERE("company_name = " + SqlUtils.formatValue(companyName))
                .toString();
        String res = companyBase.queryForObject(sql, rs -> rs.getString(1));
        return TycUtils.isUnsignedId(res) ? res : "";
    }

    public void sink(Map<String, Object> columnMap) {
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
        String sql = new SQL().REPLACE_INTO(SINK_TABLE)
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
        sink.update(sql);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> query(String uuid) {
        List<Map<String, Object>> maps = new ArrayList<>();
        String sql = new SQL().SELECT("entities")
                .FROM("bid")
                .WHERE("uuid = " + SqlUtils.formatValue(uuid))
                .toString();
        String res = gauss.queryForObject(sql, rs -> rs.getString(1));
        if (res == null) {
            return maps;
        }
        List<Object> objects = JsonUtils.parseJsonArr(res);
        for (Object object : objects) {
            maps.add((Map<String, Object>) object);
        }
        return maps;
    }

    public void delete(String id) {
        String sql = new SQL().DELETE_FROM(SINK_TABLE)
                .WHERE("id = " + SqlUtils.formatValue(id))
                .toString();
        sink.update(sql);
    }
}