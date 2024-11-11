package com.syncdb.core.util;

import com.syncdb.core.models.Record;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TestRecordsUtils {

    public static List<Record<String, String>> getTestRecords(int numRecords) {
        assert numRecords < 100;

        List<Record<String, String>> li = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            li.add(
                    Record.<String, String>builder()
                            .key("key" + (i < 10 ? "0" + i : i))  // Format "keyXX"
                            .value("value" + (i < 10 ? "0" + i : i))  // Format "valueXX"
                            .build()
            );
        }
        return li;
    }
}
