package com.liang.common.service.connector.storage.obs;

import com.liang.common.service.connector.AbstractCache;
import com.liang.common.service.connector.Logging;
import com.obs.services.ObsClient;
import com.obs.services.model.ModifyObjectRequest;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/*
drop table test.lt;
create external table test.lt
(
    id string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'obs://hadoop-obs/flink/test';
 */
@Slf4j
public class ObsWriter extends AbstractCache<String, String> {
    private final static int DEFAULT_CACHE_MILLISECONDS = 5000;
    private final static int DEFAULT_CACHE_RECORDS = 10240;
    private final static String ACCESS_KEY = "KWDCVLJRGBAVCLXGCYZD";
    private final static String SECRET_KEY = "StwOkCAdkIeQRpLsCYuAmnLEbUVhvpP7xtGaEGx2";
    private final static String END_POINT = "obs.cn-north-4.myhuaweicloud.com";

    @Getter
    private final ObsClient client = new ObsClient(ACCESS_KEY, SECRET_KEY, END_POINT);
    private final UUID uuid = UUID.randomUUID();
    private final Map<String, AtomicLong> fileToPosition = new HashMap<>();
    private final String bucket;
    private final String folder;
    private final Logging logging;
    private final FileFormat fileFormat;

    public ObsWriter(String obsUrl) {
        this(obsUrl, FileFormat.NONE);
    }

    public ObsWriter(String obsUrl, FileFormat fileFormat) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, content -> "");
        bucket = obsUrl.replaceAll("obs://(.*?)/(.*)", "$1");
        folder = obsUrl.replaceAll("obs://(.*?)/(.*)", "$2/").replaceAll("/+$", "/");
        logging = new Logging(this.getClass().getSimpleName(), folder);
        this.fileFormat = fileFormat;
    }

    @Override
    protected void updateImmediately(String ignore, Collection<String> rows) {
        logging.beforeExecute();
        String currentDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String fileName = String.format("%s-%s%s", uuid, currentDate, fileFormat.suffix);
        try {
            String objectKey = folder + fileName;
            if (!fileToPosition.containsKey(fileName)) {
                fileToPosition.clear();
                long position = 0L;
                try {
                    position = client.getObjectMetadata(bucket, objectKey).getNextPosition();
                } catch (Exception ignored) {
                }
                fileToPosition.put(fileName, new AtomicLong(position));
            }
            AtomicLong position = fileToPosition.get(fileName);
            ModifyObjectRequest request = new ModifyObjectRequest();
            request.setBucketName(bucket);
            request.setObjectKey(objectKey);
            byte[] bytes = (String.join("\n", rows) + "\n").getBytes(StandardCharsets.UTF_8);
            request.setInput(new ByteArrayInputStream(bytes));
            request.setPosition(position.getAndAdd(bytes.length));
            client.modifyObject(request);
            logging.afterExecute("write", fileName + "(" + rows.size() + "条)");
        } catch (Exception e) {
            logging.ifError("write", fileName, e);
        }
    }

    @AllArgsConstructor
    public enum FileFormat {
        NONE(""), TXT(".txt"), CSV(".csv"), JSON(".json");
        private final String suffix;
    }
}
