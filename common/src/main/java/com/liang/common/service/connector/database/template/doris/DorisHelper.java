package com.liang.common.service.connector.database.template.doris;

import com.liang.common.dto.config.DorisConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.HttpHeaders.EXPECT;

@Slf4j
@RequiredArgsConstructor
class DorisHelper {
    // stream load
    private static final String ABSTRACT_URI = "http://%s/api/%s/%s/_stream_load";
    private static final String EXPECT_VALUE = "100-continue";
    // execute put
    private static final int SUCCESS_STATUS_CODE = 200;
    private static final String SUCCESS_STATUS = "Success";
    private static final String SUCCESS_MESSAGE = "OK";
    private static final String PUBLISH_TIMEOUT_STATUS = "Publish Timeout";
    private static final String PUBLISH_TIMEOUT_MESSAGE = "PUBLISH_TIMEOUT";
    private static final int MAX_TRY_TIMES = 3;
    private static final int RETRY_INTERVAL_MILLISECOND = 1000;
    private static final int STREAM_LOAD_TIMEOUT_SECONDS = 300;
    private static final RequestConfig REQUEST_CONFIG = RequestConfig
            .custom()
            .setConnectionRequestTimeout(STREAM_LOAD_TIMEOUT_SECONDS * 1000)
            .setSocketTimeout(STREAM_LOAD_TIMEOUT_SECONDS * 1000)
            .setConnectTimeout(STREAM_LOAD_TIMEOUT_SECONDS * 1000)
            .build();
    private static final HttpClientBuilder HTTP_CLIENT_BUILDER = HttpClients
            .custom()
            .setDefaultRequestConfig(REQUEST_CONFIG)
            .setRedirectStrategy(new DorisRedirectStrategy());
    // next fe
    private final AtomicLong fePointer = new AtomicLong(0);
    // doris config
    private final DorisConfig dorisConfig;
    // auth
    private String basicAuth;

    public void executePut(String database, String table, Consumer<HttpPut> httpPutSetter) {
        HttpPut put = initPut();
        httpPutSetter.accept(put);
        try (CloseableHttpClient client = HTTP_CLIENT_BUILDER.build()) {
            int tryTimes = MAX_TRY_TIMES;
            while (tryTimes-- > 0) {
                put.setURI(getUri(database, table));
                put.setHeader("label", getLabel(database, table));
                try (CloseableHttpResponse response = client.execute(put)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    String loadResult = EntityUtils.toString(response.getEntity());
                    if (statusCode == SUCCESS_STATUS_CODE && loadResult.contains(SUCCESS_STATUS) && loadResult.contains(SUCCESS_MESSAGE)) { // Status = Success, Message = OK
                        log.info("stream load success, loadResult:\n{}", loadResult);
                        tryTimes = 0;
                    } else if (statusCode == SUCCESS_STATUS_CODE && loadResult.contains(PUBLISH_TIMEOUT_STATUS) && loadResult.contains(PUBLISH_TIMEOUT_MESSAGE)) { // Status = Publish Timeout, Message = PUBLISH_TIMEOUT
                        log.warn("stream load success, loadResult:\n{}", loadResult);
                        tryTimes = 0;
                    } else if (tryTimes == 0) {
                        log.error("stream load failed for {} times, statusCode: {}, loadResult:\n{}", MAX_TRY_TIMES, statusCode, loadResult);
                    } else {
                        LockSupport.parkUntil(System.currentTimeMillis() + RETRY_INTERVAL_MILLISECOND);
                    }
                }
            }
        } catch (Exception e) {
            log.error("stream load failed without loadResult", e);
        }
    }

    private HttpPut initPut() {
        HttpPut put = new HttpPut();
        put.setHeader(EXPECT, EXPECT_VALUE);
        put.setHeader(AUTHORIZATION, getBasicAuth());
        put.setHeader("timeout", String.valueOf(STREAM_LOAD_TIMEOUT_SECONDS));
        return put;
    }

    private String getBasicAuth() {
        if (basicAuth == null) {
            String tobeEncode = dorisConfig.getUser() + ":" + dorisConfig.getPassword();
            byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
            basicAuth = "Basic " + new String(encoded);
        }
        return basicAuth;
    }

    private URI getUri(String database, String table) {
        String nextFe = getNextFe();
        return URI.create(String.format(ABSTRACT_URI, nextFe, database, table));
    }

    private String getNextFe() {
        List<String> feList = dorisConfig.getFe();
        return feList.get((int) (fePointer.getAndIncrement() % feList.size()));
    }

    private String getLabel(String database, String table) {
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        return String.format("%s_%s_%s_%s", database, table,
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")), uuid);
    }

    @Slf4j
    private static final class DorisRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        protected boolean isRedirectable(String method) {
            return true;
        }
    }
}
