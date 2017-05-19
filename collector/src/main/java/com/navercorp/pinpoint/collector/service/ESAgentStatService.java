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
import com.navercorp.pinpoint.common.server.bo.stat.AgentStatDataPoint;
import com.navercorp.pinpoint.common.util.CollectionUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

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

        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
        add(agentStatBo.getJvmGcBos(),bulkRequest);
        add(agentStatBo.getJvmGcDetailedBos(),bulkRequest);
        add(agentStatBo.getActiveTraceBos(),bulkRequest);
        add(agentStatBo.getCpuLoadBos(),bulkRequest);
        add(agentStatBo.getResponseTimeBos(),bulkRequest);
        add(agentStatBo.getTransactionBos(),bulkRequest);
        add(agentStatBo.getDataSourceListBos(),bulkRequest);

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            logger.warn("Insert agentStat fail. {}", bulkResponse.buildFailureMessage());
        }
        logger.debug("Insert agentStat. {}", agentStatBo);

    }

    private <T extends AgentStatDataPoint> void add(List<T> list, BulkRequestBuilder bulkRequest) {
        ObjectMapper mapper = new ObjectMapper();
        if (!CollectionUtils.isEmpty(list)) {
            for (T t : list) {
                try {
                    bulkRequest.add(transportClient.prepareIndex(AGENT_STAT_VER2.getNameAsString().toLowerCase(), AGENT_STAT_VER2.getNameAsString().toLowerCase())
                            .setSource(mapper.writeValueAsBytes(t), XContentType.JSON));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
