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
import com.navercorp.pinpoint.collector.dao.StringMetaDataDao;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.server.bo.StringMetaDataBo;
import com.navercorp.pinpoint.thrift.dto.TStringMetaData;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.STRING_METADATA;
import static com.navercorp.pinpoint.common.hbase.HBaseTables.TRACE_V2;

/**
 * @author emeroad
 * @author minwoo.jung
 */
@Repository
public class ESStringMetaDataDao implements StringMetaDataDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    @Qualifier("metadataRowKeyDistributor")
    private RowKeyDistributorByHashPrefix rowKeyDistributorByHashPrefix;

    @Resource(name = "client")
    TransportClient transportClient;
    @Override
    public void insert(TStringMetaData stringMetaData) {
        if (stringMetaData == null) {
            throw new NullPointerException("stringMetaData must not be null");
        }
        if (logger.isDebugEnabled()) {
            logger.debug("insert:{}", stringMetaData);
        }

        final StringMetaDataBo stringMetaDataBo = new StringMetaDataBo(stringMetaData.getAgentId(), stringMetaData.getAgentStartTime(), stringMetaData.getStringId());
        final byte[] rowKey = getDistributedKey(stringMetaDataBo.toRowKey());


        Put put = new Put(rowKey);
        String stringValue = stringMetaData.getStringValue();
        byte[] sqlBytes = Bytes.toBytes(stringValue);
        put.addColumn(HBaseTables.STRING_METADATA_CF_STR, HBaseTables.STRING_METADATA_CF_STR_QUALI_STRING, sqlBytes);

        //hbaseTemplate.put(HBaseTables.STRING_METADATA, put);
        ObjectMapper mapper = new ObjectMapper();
        try {
            byte[] json = mapper.writeValueAsBytes(stringMetaDataBo);
            IndexResponse response = transportClient.prepareIndex(STRING_METADATA.getNameAsString().toLowerCase(),STRING_METADATA.getNameAsString().toLowerCase())
                    .setSource(json, XContentType.JSON)
                    .get();
            response.status();

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private byte[] getDistributedKey(byte[] rowKey) {
        return rowKeyDistributorByHashPrefix.getDistributedKey(rowKey);
    }
}
