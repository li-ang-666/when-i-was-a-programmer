package com.liang.common.service.connector.database.factory;

public interface SinglePoolFactory<CONFIG, POOL> {
    POOL createPool(CONFIG config);

    POOL createPool(String name);
}
