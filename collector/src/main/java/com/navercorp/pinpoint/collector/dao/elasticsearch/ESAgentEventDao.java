/*
 * Copyright 2015 NAVER Corp.
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
import com.navercorp.pinpoint.collector.dao.AgentEventDao;
import com.navercorp.pinpoint.common.server.bo.AgentEventBo;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.AGENT_EVENT;

/**
 * @author HyunGil Jeong
 */
@Repository
public class ESAgentEventDao implements AgentEventDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Resource(name = "client")
    TransportClient transportClient;

    @Override
    public void insert(AgentEventBo agentEventBo) {
        if (agentEventBo == null) {
            throw new NullPointerException("agentEventBo must not be null");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("insert event. {}", agentEventBo.toString());
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            byte[] json = mapper.writeValueAsBytes(agentEventBo);
            IndexResponse response = transportClient.prepareIndex(AGENT_EVENT.getNameAsString().toLowerCase(),AGENT_EVENT.getNameAsString().toLowerCase())
                    .setSource(json, XContentType.JSON)
                    .get();
            response.status();

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

}
