/*
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.collector.dao.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.navercorp.pinpoint.collector.dao.HostApplicationMapDao;
import com.navercorp.pinpoint.collector.util.AtomicLongUpdateMap;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.server.util.AcceptedTimeService;
import com.navercorp.pinpoint.common.util.TimeSlot;
import com.navercorp.pinpoint.common.util.TimeUtils;
import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

import java.util.HashMap;
import java.util.Map;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.AGENT_LIFECYCLE;
import static com.navercorp.pinpoint.common.hbase.HBaseTables.HOST_APPLICATION_MAP_VER2;

/**
 * @author netspider
 * @author emeroad
 */
@Repository
public class ESHostApplicationMapDao implements HostApplicationMapDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private AcceptedTimeService acceptedTimeService;

    @Autowired
    private TimeSlot timeSlot;

    @Autowired
    @Qualifier("acceptApplicationRowKeyDistributor")
    private AbstractRowKeyDistributor rowKeyDistributor;

    // FIXME should modify to save a cachekey at each 30~50 seconds instead of saving at each time
    private final AtomicLongUpdateMap<CacheKey> updater = new AtomicLongUpdateMap<>();

    @Resource(name = "client")
    TransportClient transportClient;


    @Override
    public void insert(String host, String bindApplicationName, short bindServiceType, String parentApplicationName, short parentServiceType) {
        if (host == null) {
            throw new NullPointerException("host must not be null");
        }
        if (bindApplicationName == null) {
            throw new NullPointerException("bindApplicationName must not be null");
        }

        final long statisticsRowSlot = getSlotTime();

        final CacheKey cacheKey = new CacheKey(host, bindApplicationName, bindServiceType, parentApplicationName, parentServiceType);
        final boolean needUpdate = updater.update(cacheKey, statisticsRowSlot);
        if (needUpdate) {
            insertHostVer2(host, bindApplicationName, bindServiceType, statisticsRowSlot, parentApplicationName, parentServiceType);
        }
    }


    private long getSlotTime() {
        final long acceptedTime = acceptedTimeService.getAcceptedTime();
        return timeSlot.getTimeSlot(acceptedTime);
    }


    private void insertHostVer2(String host, String bindApplicationName, short bindServiceType, long statisticsRowSlot, String parentApplicationName, short parentServiceType) {
        if (logger.isDebugEnabled()) {
            logger.debug("Insert host-application map. host={}, bindApplicationName={}, bindServiceType={}, parentApplicationName={}, parentServiceType={}",
                    host, bindApplicationName, bindServiceType, parentApplicationName, parentServiceType);
        }

        // TODO should consider to add bellow codes again later.
        //String parentAgentId = null;
        //final byte[] rowKey = createRowKey(parentApplicationName, parentServiceType, statisticsRowSlot, parentAgentId);
        final byte[] rowKey = createRowKey(parentApplicationName, parentServiceType, statisticsRowSlot, null);

        byte[] columnName = createColumnName(host, bindApplicationName, bindServiceType);

        //try {
        //    hbaseTemplate.put(HBaseTables.HOST_APPLICATION_MAP_VER2, rowKey, HBaseTables.HOST_APPLICATION_MAP_VER2_CF_MAP, columnName, null);
        //} catch (Exception ex) {
        //    logger.warn("retry one. Caused:{}", ex.getCause(), ex);
        //    hbaseTemplate.put(HBaseTables.HOST_APPLICATION_MAP_VER2, rowKey, HBaseTables.HOST_APPLICATION_MAP_VER2_CF_MAP, columnName, null);
        //}
        ObjectMapper mapper = new ObjectMapper();

        Map json = new HashMap();
        json.put("parentApplicationName", parentApplicationName);
        json.put("parentServiceType", parentServiceType);
        json.put("statisticsRowSlot", TimeUtils.reverseTimeMillis(statisticsRowSlot));
        json.put("host", host);
        json.put("bindApplicationName", bindApplicationName);
        json.put("bindServiceType", bindServiceType);
        IndexResponse response = transportClient.prepareIndex(HOST_APPLICATION_MAP_VER2.getNameAsString().toLowerCase(), HOST_APPLICATION_MAP_VER2.getNameAsString().toLowerCase())
                .setSource(json, XContentType.JSON)
                .get();
        response.status();


    }

    private byte[] createColumnName(String host, String bindApplicationName, short bindServiceType) {
        Buffer buffer = new AutomaticBuffer();
        buffer.putPrefixedString(host);
        buffer.putPrefixedString(bindApplicationName);
        buffer.putShort(bindServiceType);
        return buffer.getBuffer();
    }


    private byte[] createRowKey(String parentApplicationName, short parentServiceType, long statisticsRowSlot, String parentAgentId) {
        final byte[] rowKey = createRowKey0(parentApplicationName, parentServiceType, statisticsRowSlot, parentAgentId);
        return rowKeyDistributor.getDistributedKey(rowKey);
    }

    byte[] createRowKey0(String parentApplicationName, short parentServiceType, long statisticsRowSlot, String parentAgentId) {

        // even if  a agentId be added for additional specifications, it may be safe to scan rows.
        // But is it needed to add parentAgentServiceType?
        final int SIZE = HBaseTables.APPLICATION_NAME_MAX_LEN + 2 + 8;
        final Buffer rowKeyBuffer = new AutomaticBuffer(SIZE);
        rowKeyBuffer.putPadString(parentApplicationName, HBaseTables.APPLICATION_NAME_MAX_LEN);
        rowKeyBuffer.putShort(parentServiceType);
        rowKeyBuffer.putLong(TimeUtils.reverseTimeMillis(statisticsRowSlot));
        // there is no parentAgentId for now.  if it added later, need to comment out below code for compatibility.
//        rowKeyBuffer.putPadString(parentAgentId, HBaseTables.AGENT_NAME_MAX_LEN);
        return rowKeyBuffer.getBuffer();
    }

    private static final class CacheKey {
        private final String host;
        private final String applicationName;
        private final short serviceType;

        private final String parentApplicationName;
        private final short parentServiceType;

        public CacheKey(String host, String applicationName, short serviceType, String parentApplicationName, short parentServiceType) {
            if (host == null) {
                throw new NullPointerException("host must not be null");
            }
            if (applicationName == null) {
                throw new NullPointerException("bindApplicationName must not be null");
            }
            this.host = host;
            this.applicationName = applicationName;
            this.serviceType = serviceType;

            // may be null for below two parent values.
            this.parentApplicationName = parentApplicationName;
            this.parentServiceType = parentServiceType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            if (parentServiceType != cacheKey.parentServiceType) return false;
            if (serviceType != cacheKey.serviceType) return false;
            if (!applicationName.equals(cacheKey.applicationName)) return false;
            if (!host.equals(cacheKey.host)) return false;
            if (parentApplicationName != null ? !parentApplicationName.equals(cacheKey.parentApplicationName) : cacheKey.parentApplicationName != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = host.hashCode();
            result = 31 * result + applicationName.hashCode();
            result = 31 * result + (int) serviceType;
            result = 31 * result + (parentApplicationName != null ? parentApplicationName.hashCode() : 0);
            result = 31 * result + (int) parentServiceType;
            return result;
        }
    }
}