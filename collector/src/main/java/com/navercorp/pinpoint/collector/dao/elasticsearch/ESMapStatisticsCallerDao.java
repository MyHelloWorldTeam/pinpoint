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
import com.google.common.util.concurrent.AtomicLongMap;
import com.navercorp.pinpoint.collector.dao.MapStatisticsCallerDao;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.*;
import com.navercorp.pinpoint.collector.util.AtomicLongMapUtils;
import com.navercorp.pinpoint.common.server.util.AcceptedTimeService;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.util.ApplicationMapStatisticsUtils;
import com.navercorp.pinpoint.common.util.TimeSlot;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Increment;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
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
import java.util.List;
import java.util.Map;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.*;
import static com.navercorp.pinpoint.common.hbase.HBaseTables.MAP_STATISTICS_CALLER_VER2;

/**
 * Update statistics of caller node
 *
 * @author netspider
 * @author emeroad
 */
@Repository
public class ESMapStatisticsCallerDao implements MapStatisticsCallerDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private AcceptedTimeService acceptedTimeService;

    @Autowired
    @Qualifier("callerMerge")
    private RowKeyMerge rowKeyMerge;

    @Autowired
    @Qualifier("statisticsCallerRowKeyDistributor")
    private RowKeyDistributorByHashPrefix rowKeyDistributorByHashPrefix;

    @Resource(name = "client")
    TransportClient transportClient;

    @Autowired
    private TimeSlot timeSlot;

    private final boolean useBulk;

    private final AtomicLongMap<RowInfo> counter = AtomicLongMap.create();

    public ESMapStatisticsCallerDao() {
        this(true);
    }

    public ESMapStatisticsCallerDao(boolean useBulk) {
        this.useBulk = useBulk;
    }

    ObjectMapper mapper = new ObjectMapper();

    @Override
    public void update(String callerApplicationName, ServiceType callerServiceType, String callerAgentid, String calleeApplicationName, ServiceType calleeServiceType, String calleeHost, int elapsed, boolean isError) {
        if (callerApplicationName == null) {
            throw new NullPointerException("callerApplicationName must not be null");
        }
        if (calleeApplicationName == null) {
            throw new NullPointerException("calleeApplicationName must not be null");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[Caller] {} ({}) {} -> {} ({})[{}]", callerApplicationName, callerServiceType, callerAgentid,
                    calleeApplicationName, calleeServiceType, calleeHost);
        }

        // there may be no endpoint in case of httpclient
        calleeHost = StringUtils.defaultString(calleeHost);

        // make row key. rowkey is me
        final long acceptedTime = acceptedTimeService.getAcceptedTime();
        final long rowTimeSlot = timeSlot.getTimeSlot(acceptedTime);
        final RowKey callerRowKey = new CallRowKey(callerApplicationName, callerServiceType.getCode(), rowTimeSlot);

        final short calleeSlotNumber = ApplicationMapStatisticsUtils.getSlotNumber(calleeServiceType, elapsed, isError);
        final ColumnName calleeColumnName = new CalleeColumnName(callerAgentid, calleeServiceType.getCode(), calleeApplicationName, calleeHost, calleeSlotNumber);
        if (useBulk) {
            RowInfo rowInfo = new DefaultRowInfo(callerRowKey, calleeColumnName);
            this.counter.incrementAndGet(rowInfo);
        } else {
            final byte[] rowKey = getDistributedKey(callerRowKey.getRowKey());
            // column name is the name of caller app.
            byte[] columnName = calleeColumnName.getColumnName();
            increment(rowKey, columnName, 1L);
        }
    }

    private void increment(byte[] rowKey, byte[] columnName, long increment) {
        if (rowKey == null) {
            throw new NullPointerException("rowKey must not be null");
        }
        if (columnName == null) {
            throw new NullPointerException("columnName must not be null");
        }
        //hbaseTemplate.incrementColumnValue(MAP_STATISTICS_CALLEE_VER2, rowKey, MAP_STATISTICS_CALLEE_VER2_CF_COUNTER, columnName, increment);


    }

    @Override
    public void flushAll() {
        if (!useBulk) {
            throw new IllegalStateException();
        }
        // update statistics by rowkey and column for now. need to update it by rowkey later.
        final Map<RowInfo, Long> remove = AtomicLongMapUtils.remove(this.counter);

        final List<Increment> merge = rowKeyMerge.createBulkIncrement(remove, rowKeyDistributorByHashPrefix);
        if (merge.isEmpty()) {
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("flush {} Increment:{}", this.getClass().getSimpleName(), merge.size());
        }

        //hbaseTemplate.increment(MAP_STATISTICS_CALLEE_VER2, merge);
        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
        remove.entrySet().stream().forEach(p->{
            Map json = new HashMap();
            RowInfo r = p.getKey();
            CallRowKey cr = (CallRowKey) r.getRowKey();
            CalleeColumnName rc = (CalleeColumnName) r.getColumnName();
            Long l = p.getValue();
            json.put("callApplicationName",cr.callApplicationName);
            json.put("callServiceType",cr.callServiceType);
            json.put("rowTimeSlot",cr.rowTimeSlot);
            json.put("calleeApplicationName",rc.calleeApplicationName);
            json.put("calleeServiceType",rc.calleeServiceType);
            json.put("callerAgentId",rc.callerAgentId);
            json.put("callHost",rc.callHost);
            json.put("columnSlotNumber",rc.columnSlotNumber);
            json.put("slotNumber",l);
            bulkRequest.add(transportClient.prepareIndex(MAP_STATISTICS_CALLEE_VER2.getNameAsString().toLowerCase(), MAP_STATISTICS_CALLEE_VER2.getNameAsString().toLowerCase())
                    .setSource(json, XContentType.JSON));
        });
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            logger.warn("Insert MAP_STATISTICS_CALLEE fail. {}", bulkResponse.buildFailureMessage());
        }
    }

    private byte[] getDistributedKey(byte[] rowKey) {
        return rowKeyDistributorByHashPrefix.getDistributedKey(rowKey);
    }
}