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
import com.navercorp.pinpoint.collector.dao.ApiMetaDataDao;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.server.bo.ApiMetaDataBo;
import com.navercorp.pinpoint.thrift.dto.TApiMetaData;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.hbase.client.Put;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.API_METADATA;
import static com.navercorp.pinpoint.common.hbase.HBaseTables.API_METADATA_CF_API;

/**
 * @author emeroad
 * @author minwoo.jung
 */
@Repository
public class ESApiMetaDataDao implements ApiMetaDataDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    @Qualifier("metadataRowKeyDistributor")
    private RowKeyDistributorByHashPrefix rowKeyDistributorByHashPrefix;

    @Resource(name = "client")
    TransportClient transportClient;

    @Override
    public void insert(TApiMetaData apiMetaData) {
        if (logger.isDebugEnabled()) {
            logger.debug("insert:{}", apiMetaData);
        }


        ApiMetaDataBo apiMetaDataBo = new ApiMetaDataBo(apiMetaData.getAgentId(), apiMetaData.getAgentStartTime(), apiMetaData.getApiId());
        byte[] rowKey = getDistributedKey(apiMetaDataBo.toRowKey());

        final Put put = new Put(rowKey);

        final Buffer buffer = new AutomaticBuffer(64);
        String api = apiMetaData.getApiInfo();
        buffer.putPrefixedString(api);
        if (apiMetaData.isSetLine()) {
            buffer.putInt(apiMetaData.getLine());
        } else {
            buffer.putInt(-1);
        }
        if(apiMetaData.isSetType()) {
            buffer.putInt(apiMetaData.getType());
        } else {
            buffer.putInt(0);
        }
        
        final byte[] apiMetaDataBytes = buffer.getBuffer();
        put.addColumn(API_METADATA_CF_API, HBaseTables.API_METADATA_CF_API_QUALI_SIGNATURE, apiMetaDataBytes);

        //hbaseTemplate.put(HBaseTables.API_METADATA, put);

        ObjectMapper mapper = new ObjectMapper();
        try {
            byte[] json = mapper.writeValueAsBytes(apiMetaDataBo);
            IndexResponse response = transportClient.prepareIndex(API_METADATA.getNameAsString().toLowerCase(),API_METADATA.getNameAsString().toLowerCase())
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
