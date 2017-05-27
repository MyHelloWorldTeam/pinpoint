package com.navercorp.pinpoint.collector.dao.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.navercorp.pinpoint.collector.dao.ApplicationIndexDao;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

import java.util.HashMap;
import java.util.Map;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.APPLICATION_INDEX;
import static com.navercorp.pinpoint.common.hbase.HBaseTables.APPLICATION_INDEX_CF_AGENTS;

/**
 * Created by william on 2017/5/16.
 */
@Repository
public class ESApplicationIndexDao implements ApplicationIndexDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Resource(name = "client")
    TransportClient transportClient;

    @Override
    public void insert(final TAgentInfo agentInfo) {
        if (agentInfo == null) {
            throw new NullPointerException("agentInfo must not be null");
        }

        Map json = new HashMap();
        json.put("applicationName", agentInfo.getApplicationName());
        json.put("agentId", agentInfo.getAgentId());
        json.put("serviceType", agentInfo.getServiceType());
        IndexResponse response = transportClient.prepareIndex(APPLICATION_INDEX.getNameAsString().toLowerCase(), APPLICATION_INDEX.getNameAsString().toLowerCase())
                .setSource(json, XContentType.JSON)
                .get();
        response.status();

        logger.debug("Insert agentInfo. {}", agentInfo);

    }
}
