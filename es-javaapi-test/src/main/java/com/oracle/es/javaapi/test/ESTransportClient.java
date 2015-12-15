package com.oracle.es.javaapi.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Wrapper around an ElasticSearch {@link TransportClient} node.
 * 
 * @author hugovalk
 * 
 */
public class ESTransportClient implements ESClient {

    private TransportClient client;

    /**
     * Default constructor, initializing the transport client.
     */
    public ESTransportClient(){
	final Settings settings = Settings.settingsBuilder()
                .put("path.home", "/home/oracle/elasticsearch-2.1.0")
		.put("client.transport.sniff", true)
		.put("cluster.name", "elasticsearch").build();
        try {
            client = TransportClient.builder()
                    .settings(settings)
                    .build()
                    .addTransportAddress(new InetSocketTransportAddress(
                            InetAddress.getByName("127.0.0.1"), 9300));
        } catch (UnknownHostException ex) {
            Logger.getLogger(ESTransportClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Client getClient() {
	return client;
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown() {
	client.close();
	client = null;
    }

}
