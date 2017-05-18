/*
 * Copyright 2017 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.navercorp.pinpoint.collector.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.navercorp.pinpoint.common.server.bo.stat.AgentStatBo;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.AGENT_STAT_VER2;

/**
 * @author minwoo.jung
 */
@Service("eSAgentStatService")
public class ESAgentStatService implements AgentStatService {

    private final Logger logger = LoggerFactory.getLogger(ESAgentStatService.class.getName());

    @Resource(name = "client")
    TransportClient transportClient;

    @Override
    public void save(AgentStatBo agentStatBo) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
            bulkRequest.add(transportClient.prepareIndex(AGENT_STAT_VER2.getNameAsString(),AGENT_STAT_VER2.getNameAsString())
                    .setSource(mapper.writeValueAsBytes(agentStatBo.getJvmGcBos()), XContentType.JSON));
            bulkRequest.add(transportClient.prepareIndex(AGENT_STAT_VER2.getNameAsString(),AGENT_STAT_VER2.getNameAsString())
                    .setSource(mapper.writeValueAsBytes(agentStatBo.getJvmGcDetailedBos()), XContentType.JSON));
            bulkRequest.add(transportClient.prepareIndex(AGENT_STAT_VER2.getNameAsString(),AGENT_STAT_VER2.getNameAsString())
                    .setSource(mapper.writeValueAsBytes(agentStatBo.getActiveTraceBos()), XContentType.JSON));
            bulkRequest.add(transportClient.prepareIndex(AGENT_STAT_VER2.getNameAsString(),AGENT_STAT_VER2.getNameAsString())
                    .setSource(mapper.writeValueAsBytes(agentStatBo.getCpuLoadBos()), XContentType.JSON));
            bulkRequest.add(transportClient.prepareIndex(AGENT_STAT_VER2.getNameAsString(),AGENT_STAT_VER2.getNameAsString())
                    .setSource(mapper.writeValueAsBytes(agentStatBo.getResponseTimeBos()), XContentType.JSON));
            bulkRequest.add(transportClient.prepareIndex(AGENT_STAT_VER2.getNameAsString(),AGENT_STAT_VER2.getNameAsString())
                    .setSource(mapper.writeValueAsBytes(agentStatBo.getTransactionBos()), XContentType.JSON));
            bulkRequest.add(transportClient.prepareIndex(AGENT_STAT_VER2.getNameAsString(),AGENT_STAT_VER2.getNameAsString())
                    .setSource(mapper.writeValueAsBytes(agentStatBo.getDataSourceListBos()), XContentType.JSON));
            BulkResponse bulkResponse = bulkRequest.get();
            if(bulkResponse.hasFailures()){

            }
            logger.debug("Insert agentStat. {}", agentStatBo);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
