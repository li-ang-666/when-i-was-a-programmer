// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.liang.udf;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.connector.database.template.doris.DorisWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DorisBitmapUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Collections;

@Description(name = "bitmap_to_doris", value = "_FUNC_( crowd_id, create_timestamp, user_id_bitmap, env('TEST' or 'PROD') ) - flush bitmap to doris's crowd_user_bitmap table, Returns the number flush to doris")
public class BitmapToDorisUDF extends GenericUDF {
    private static final DorisSchema DORIS_SCHEMA_PROD = DorisSchema.builder()
            .database("crowd").tableName("crowd_user_bitmap")
            .derivedColumns(Collections.singletonList("user_id_bitmap = if(user_id < 1, bitmap_empty(), to_bitmap(user_id))"))
            .build();
    private static final DorisSchema DORIS_SCHEMA_TEST = DorisSchema.builder()
            .database("test").tableName("crowd_user_bitmap")
            .derivedColumns(Collections.singletonList("user_id_bitmap = if(user_id < 1, bitmap_empty(), to_bitmap(user_id))"))
            .build();
    private transient LongObjectInspector inputOI0;
    private transient LongObjectInspector inputOI1;
    private transient BinaryObjectInspector inputOI2;
    private transient StringObjectInspector inputOI3;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) {
        ObjectInspector input0 = arguments[0];
        if (!(input0 instanceof LongObjectInspector)) {
            throw new RuntimeException("the first argument must be a bigint (crowd_id)");
        }
        ObjectInspector input1 = arguments[1];
        if (!(input1 instanceof LongObjectInspector)) {
            throw new RuntimeException("the second argument must be a bigint (create_timestamp)");
        }
        ObjectInspector input2 = arguments[2];
        if (!(input2 instanceof BinaryObjectInspector)) {
            throw new RuntimeException("the third argument must be a binary (user_id_bitmap)");
        }
        ObjectInspector input3 = arguments[3];
        if (!(input3 instanceof StringObjectInspector)) {
            throw new RuntimeException("the fourth argument must be a string ('TEST' or 'PROD')");
        }
        this.inputOI0 = (LongObjectInspector) input0;
        this.inputOI1 = (LongObjectInspector) input1;
        this.inputOI2 = (BinaryObjectInspector) input2;
        this.inputOI3 = (StringObjectInspector) input3;
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) {
        ConfigUtils.setConfig(ConfigUtils.createConfig(""));
        DorisWriter dorisWriter = new DorisWriter("dorisSink", 128 * 1024 * 1024);
        try {
            String crowdId = String.valueOf(this.inputOI0.getPrimitiveJavaObject(args[0].get()));
            String createTimestamp = String.valueOf(this.inputOI1.getPrimitiveJavaObject(args[1].get()));
            Roaring64NavigableMap userIdBitmap = DorisBitmapUtils.parseBinary((this.inputOI2.getPrimitiveJavaObject(args[2].get())));
            String env = String.valueOf(this.inputOI3.getPrimitiveJavaObject(args[3].get()));
            DorisSchema currentDorisSchema;
            if ("PROD".equals(env)) {
                currentDorisSchema = DORIS_SCHEMA_PROD;
            } else if ("TEST".equals(env)) {
                currentDorisSchema = DORIS_SCHEMA_TEST;
            } else {
                throw new RuntimeException("the fourth argument must be 'TEST' or 'PROD'");
            }
            if (userIdBitmap.isEmpty()) {
                DorisOneRow dorisOneRow = new DorisOneRow(currentDorisSchema)
                        .put("crowd_id", crowdId)
                        .put("create_timestamp", createTimestamp)
                        .put("user_id", 0);
                dorisWriter.write(dorisOneRow);
                dorisWriter.flush();
                return 0L;
            }
            userIdBitmap.forEach(userId -> {
                DorisOneRow dorisOneRow = new DorisOneRow(currentDorisSchema)
                        .put("crowd_id", crowdId)
                        .put("create_timestamp", createTimestamp)
                        .put("user_id", userId);
                dorisWriter.write(dorisOneRow);
            });
            dorisWriter.flush();
            return userIdBitmap.getLongCardinality();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: bitmap_to_doris( crowd_id, create_timestamp, user_id_bitmap, env('TEST' or 'PROD') )";
    }
}