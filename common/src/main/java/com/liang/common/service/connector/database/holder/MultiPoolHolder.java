package com.liang.common.service.connector.database.holder;

public interface MultiPoolHolder<POOL> {
    POOL getPool(String name);

    void closeAll();
}
