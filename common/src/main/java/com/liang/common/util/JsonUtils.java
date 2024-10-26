package com.liang.common.util;

import com.liang.shaded.com.fasterxml.jackson.core.JsonGenerator;
import com.liang.shaded.com.fasterxml.jackson.core.JsonParser;
import com.liang.shaded.com.fasterxml.jackson.core.StreamReadConstraints;
import com.liang.shaded.com.fasterxml.jackson.core.json.JsonReadFeature;
import com.liang.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import com.liang.shaded.com.fasterxml.jackson.databind.JavaType;
import com.liang.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.liang.shaded.com.fasterxml.jackson.databind.SerializationFeature;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@Slf4j
@SuppressWarnings("unchecked")
@UtilityClass
public class JsonUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setTimeZone(TimeZone.getTimeZone("GTM+8"))
            // 可以识别单引号
            .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
            .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false).setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
            //识别控制字符
            .configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true)
            //识别不认识的控制字符
            .configure(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER.mappedFeature(), true)
            .configure(JsonReadFeature.ALLOW_JAVA_COMMENTS.mappedFeature(), true);

    static {
        // 超长
        StreamReadConstraints streamReadConstraints = StreamReadConstraints
                .builder()
                .maxStringLength(Integer.MAX_VALUE - 1)
                .build();
        OBJECT_MAPPER.getFactory().setStreamReadConstraints(streamReadConstraints);
    }

    /*----------------------------------解析{XX=XX, XX=XX, XX=XX}----------------------------------*/
    public static Map<String, Object> parseJsonObj(String json) {
        return parseJsonObj(json, Map.class);
    }

    public static <T> T parseJsonObj(String json, Class<T> clz) {
        T t;
        try {
            t = OBJECT_MAPPER.readValue(json, clz);
        } catch (Exception e) {
            log.error("JsonUtils error", e);
            t = null;
        }
        return t;
    }

    /*----------------------------------解析[XX, XX, XX]-------------------------------------------*/
    public static List<Object> parseJsonArr(String json) {
        return parseJsonArr(json, Object.class);
    }

    public static <T> List<T> parseJsonArr(String json, Class<T> clz) {
        List<T> result;
        try {
            JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametricType(List.class, clz);
            result = OBJECT_MAPPER.readValue(json, javaType);
        } catch (Exception e) {
            log.error("JsonUtils error", e);
            result = null;
        }
        return result;
    }

    /*----------------------------------序列化------------------------------------------------------*/
    public static String toString(Object o) {
        String result;
        try {
            result = OBJECT_MAPPER.writeValueAsString(o);
        } catch (Exception e) {
            log.error("JsonUtils error", e);
            result = null;
        }
        return result;
    }

    public static byte[] toBytes(Object o) {
        byte[] result;
        try {
            result = OBJECT_MAPPER.writeValueAsBytes(o);
        } catch (Exception e) {
            log.error("JsonUtils error", e);
            result = null;
        }
        return result;
    }
}
