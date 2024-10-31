package com.liang.repair.impl;

import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.repair.service.ConfigHolder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BlackList extends ConfigHolder {
    private static final ExecutorService POOL = Executors.newFixedThreadPool(128);

    public static void main(String[] args) {
        POOL.execute(deletePartner("ps://www.tianyancha.com/human/2256476524-c5989305801"));
        POOL.execute(deletePartner("https://www.tianyancha.com/human/1810538098-c424826151"));
        POOL.execute(deletePartner("https://www.tianyancha.com/human/2274126543-c3104140294"));
        POOL.execute(deletePartner("ttps://www.tianyancha.com/human/2019184620-c611107056"));
        POOL.execute(deletePartner("https://www.tianyancha.com/human/2077813474-c3070387220"));
        POOL.execute(deletePartner("https://www.tianyancha.com/human/1807415413-c1123142789"));
        POOL.execute(deletePartner("ttps://www.tianyancha.com/human/2276910784-c5362941886"));
        POOL.execute(deletePartner("https://www.tianyancha.com/human/2294397035-c1235590572", "company_gid = 2544215070"));
        POOL.execute(deletePartner("https://www.tianyancha.com/human/1818328930-c5348014077", "partner_human_name in ('刘艳','刘建','解飞')"));
        POOL.execute(deleteController("6713760382"));
        POOL.shutdown();
    }

    private static Runnable deletePartner(String pidOrUrl, String... where) {
        return () -> {
            String pid = pidOrUrl.contains("-c") ? getPidByUrl(pidOrUrl) : pidOrUrl;
            if (String.valueOf(pid).length() == 17) {
                SQL sql = new SQL().DELETE_FROM("cooperation_partner")
                        .WHERE("boss_human_pid = " + SqlUtils.formatValue(pid));
                for (String whereSyntax : where) {
                    sql.WHERE(whereSyntax);
                }
                new JdbcTemplate("467.company_base").update(sql.toString());
            }
        };
    }

    private static Runnable deleteController(String companyId) {
        return () -> {
            String sql = new SQL().DELETE_FROM("entity_controller_details_new")
                    .WHERE("company_id_controlled = " + SqlUtils.formatValue(companyId))
                    .toString();
            new JdbcTemplate("463.bdp_equity").update(sql);
        };
    }

    private static String getPidByUrl(String url) {
        String[] split = url.replaceAll("(.*?)(\\d+-c\\d+)(.*)", "$2").split("-c");
        String sql = new SQL().SELECT("human_pid")
                .FROM("company_human_relation")
                .WHERE("human_graph_id = " + SqlUtils.formatValue(split[0]))
                .WHERE("company_graph_id = " + SqlUtils.formatValue(split[1]))
                .WHERE("deleted = 0")
                .toString();
        return new JdbcTemplate("157.prism_boss").queryForObject(sql, rs -> rs.getString(1));
    }
}
