package com.liang.repair.impl;

import com.liang.repair.service.ConfigHolder;
import com.obs.services.ObsClient;

public class ReadObs extends ConfigHolder {
    private final static String endPoint = "obs.cn-north-4.myhuaweicloud.com";
    private final static String ak = "NT5EWZ4FRH54R2R2CB8G";
    private final static String sk = "BJok3jQFTmFYUS68lFWegazYggw5anKsOFUb65bS";
    private final static ObsClient obsClient = new ObsClient(ak, sk, endPoint);

    public static void main(String[] args) throws Exception {
    }
}
