package com.oracle.es.javaapi.test;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * Wrapper around an ElasticSearch {@link Node}, to obtain a {@link Client}
 * from.
 * 
 * @author hugovalk
 * 
 */
public class ESLocalNodeClient implements ESClient {

    private Node node;

    /**
     * Default constructor, initializing the client node.
     */
    public ESLocalNodeClient() {
	final Settings settings = Settings.settingsBuilder()
                .put("path.home", "/home/oracle/elasticsearch-2.1.0")
		.put("node.name", "My beautiful node").build();
        
        NodeBuilder nb = new NodeBuilder().settings(settings)
		.clusterName("elasticsearch")
		// .local(true)
 		.client(true);

	node = nb.build().start();
    }

    /** {@inheritDoc} */
    @Override
    public Client getClient() {
	return node.client();
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown() {
	getClient().close();
	node.close();
	node = null;
    }

}
