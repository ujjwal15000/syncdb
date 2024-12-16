package com.syncdb.core.util;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;

import java.io.IOException;
import java.net.InetAddress;

@Getter
@Slf4j
public class TimeUtils {
    public static Long DELTA;

    public static void init() throws IOException {
        if(DELTA == null)
            DELTA = calculateDelta();
    }

    public static Long calculateDelta() throws IOException {
        String ntpServer = "pool.ntp.org";
        try {
            NTPUDPClient client = new NTPUDPClient();
            client.setDefaultTimeout(10000);

            InetAddress hostAddr = InetAddress.getByName(ntpServer);
            TimeInfo timeInfo = client.getTime(hostAddr);
            timeInfo.computeDetails();

            return timeInfo.getOffset();
        } catch (Exception e) {
            log.error(String.format("error while fetching network time: %s", e.getMessage()), e);
            throw e;
        }
    }
    
}
