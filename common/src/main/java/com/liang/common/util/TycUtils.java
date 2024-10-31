package com.liang.common.util;

import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.liang.common.util.SqlUtils.formatValue;

@UtilityClass
public class TycUtils {
    public static boolean isUnsignedId(Object id) {
        String idStr = String.valueOf(id);
        return StringUtils.isNumeric(idStr) && !"0".equals(idStr);
    }

    public static boolean isTycUniqueEntityId(Object shareholderId) {
        // 判空
        if (shareholderId == null) {
            return false;
        }
        String shareholderIdStr = String.valueOf(shareholderId);
        int length = shareholderIdStr.length();
        if (length < 17) {
            // 小于17位, 判断数字
            return isUnsignedId(shareholderIdStr);
        } else {
            // 否则判断pid
            for (int i = 0; i < length; i++) {
                if (!Character.isLetterOrDigit(shareholderIdStr.charAt(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    public static boolean isValidName(Object name) {
        String nameStr = String.valueOf(name);
        return StringUtils.isNotBlank(nameStr) && !"null".equalsIgnoreCase(nameStr);
    }

    public static boolean isYear(Object year) {
        String str = String.valueOf(year);
        if (!str.matches("\\d{4}")) {
            return false;
        }
        int i = Integer.parseInt(str);
        return 1900 <= i && i <= Long.parseLong(DateUtils.currentDate().substring(0, 4));
    }

    public static boolean isDateTimeByNow(Object datetime) {
        String str = String.valueOf(datetime);
        return str.matches("\\d{4}-\\d{2}-\\d{2}( \\d{2}:\\d{2}:\\d{2}.*)?")
                && "1900-01-01".compareTo(str) <= 0 && str.compareTo(DateUtils.currentDatetime() + ".999999999") <= 0;
    }

    public static boolean isDateTime(Object datetime) {
        String str = String.valueOf(datetime);
        return str.matches("\\d{4}-\\d{2}-\\d{2}( \\d{2}:\\d{2}:\\d{2}.*)?")
                && "1900-01-01".compareTo(str) <= 0 && str.compareTo("9999-12-31 23:59:59.999999999") <= 0;
    }

    @NonNull
    public static Tuple2<String, String> formatEquity(Object equity) {
        // 判空
        if (equity == null) {
            return Tuple2.of(getMultiplied("-1", 1), "人民币");
        }
        String equityStr = String.valueOf(equity);
        StringBuilder numberBuilder = new StringBuilder();
        StringBuilder unitBuilder = new StringBuilder();
        int length = equityStr.length();
        for (int i = 0; i < length; i++) {
            char c = equityStr.charAt(i);
            if (Character.isDigit(c) || '.' == c) {
                numberBuilder.append(c);
            } else if (!Character.isWhitespace(c) && String.valueOf(c).matches("[\u4e00-\u9fa5]")) {
                unitBuilder.append(c);
            }
        }
        String number = numberBuilder.toString();
        String unit = unitBuilder.toString();
        if (unit.isEmpty()) {
            // 如果整数有5位
            if (getMultiplied(number, 1).split("\\.")[0].length() >= 5) {
                unit = "元";
            } else {
                unit = "万元";
            }
        } else if ("万".equals(unit)) {
            unit = "万元";
        }
        number = getMultiplied(number, unit.contains("万") ? 10000 * 10 * 10 : 10 * 10);
        if (unit.contains("人民币") || "万元".equals(unit) || "元".equals(unit)) {
            return Tuple2.of(number, "人民币");
        } else if (unit.contains("美国") || unit.contains("美元") || unit.contains("美币")) {
            return Tuple2.of(number, "美元");
        } else if (unit.contains("香港") || unit.contains("港元") || unit.contains("港币")) {
            return Tuple2.of(number, "港元");
        } else if (unit.contains("澳门") || unit.contains("澳元") || unit.contains("澳币")) {
            return Tuple2.of(number, "澳门元");
        } else if (unit.contains("台湾") || unit.contains("台元") || unit.contains("台币")) {
            return Tuple2.of(number, "新台币");
        } else if (unit.contains("日本") || unit.contains("日元") || unit.contains("日币")) {
            return Tuple2.of(number, "日元");
        } else if (unit.contains("欧洲") || unit.contains("欧元") || unit.contains("欧币")) {
            return Tuple2.of(number, "欧元");
        } else if (unit.contains("英国") || unit.contains("英镑") || unit.contains("英元") || unit.contains("英币")) {
            return Tuple2.of(number, "英镑");
        } else if (unit.contains("韩国") || unit.contains("韩元") || unit.contains("韩币")) {
            return Tuple2.of(number, "韩元");
        } else {
            return Tuple2.of(number, unit.replaceAll("万元|万", ""));
        }
    }

    @NonNull
    private static String getMultiplied(String number, long multiply) {
        try {
            BigDecimal bigDecimal = new BigDecimal(multiply);
            return new BigDecimal(number)
                    .multiply(bigDecimal)
                    .setScale(12, RoundingMode.DOWN)
                    .toPlainString();
        } catch (Exception e) {
            return new BigDecimal(-1)
                    .setScale(12, RoundingMode.DOWN)
                    .toPlainString();
        }
    }


    @NonNull
    public static String gid2Pid(Object companyGid, Object humanGid) {
        if (!isUnsignedId(companyGid) || !isUnsignedId(humanGid)) {
            return "0";
        }
        String sql = new SQL().SELECT("human_pid")
                .FROM("company_human_relation")
                .WHERE("deleted = 0")
                .WHERE("company_graph_id = " + formatValue(companyGid))
                .WHERE("human_graph_id = " + formatValue(humanGid))
                .toString();
        String res = new JdbcTemplate("157.prism_boss").queryForObject(sql, rs -> rs.getString(1));
        return isTycUniqueEntityId(res) ? res : "0";
    }

    @NonNull
    public static String pid2Name(Object humanPid) {
        if (!TycUtils.isTycUniqueEntityId(humanPid) || String.valueOf(humanPid).length() != 17) {
            return "";
        }
        String sql = new SQL().SELECT("entity_name_valid")
                .FROM("tyc_entity_main_reference")
                .WHERE("tyc_unique_entity_id = " + formatValue(humanPid))
                .toString();
        String res = new JdbcTemplate("465.company_base").queryForObject(sql, rs -> rs.getString(1));
        return isValidName(res) ? res : "";
    }
}
