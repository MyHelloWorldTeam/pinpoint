package com.navercorp.pinpoint.collector.util;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by william on 2017/5/16.
 */
public class ElasticSearchClient implements FactoryBean<TransportClient>, InitializingBean, DisposableBean {

    TransportClient client;

    String hostName;

    public ElasticSearchClient(String hostName) {
        this.hostName = hostName;

    }

    @Override
    public void destroy() throws Exception {
        client.close();
    }

    @Override
    public TransportClient getObject() throws Exception {
        return client;
    }

    @Override
    public Class<?> getObjectType() {
        return TransportClient.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (client == null) {
            try {
                client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), 9300));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
    }
}
