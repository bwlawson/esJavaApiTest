package com.oracle.es.javaapi.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Elasticsearch demo application.
 */
public class ElasticsearchDemoApp {

    private static final Logger LOG = LoggerFactory
	    .getLogger(ElasticsearchDemoApp.class);

    public static void main(String[] args) {
	new ElasticsearchDemoApp().indexDocument();
	new ElasticsearchDemoApp().getDocument();
//	new ElasticsearchDemoApp().deleteDocument();
	new ElasticsearchDemoApp().searchForDocument();
    }

    private final ESClient esClient = new ESLocalNodeClient();

    private void indexDocument() {
	final Tweet tweet = new Tweet();
	tweet.setUser("kimchy");
	tweet.setPostDate(new Date());
	tweet.setMessage("trying out Elasticsearch");

        byte[] tweetJson = null;
        try {
             // instance a json mapper
            ObjectMapper mapper = new ObjectMapper(); // create once, reuse
            
            // generate json
            tweetJson = mapper.writeValueAsBytes(tweet);
     
            IndexRequestBuilder irb = esClient.getClient().prepareIndex("twitter", "tweet", "2");
            
//            irb.setSource(jsonBuilder()
//                    .startObject()
//                        .field("user", "kimchy")
//                        .field("postDate", new Date())
//                        .field("message", "trying out Elasticsearch")
//                    .endObject()
//                  );
            
            irb.setSource(tweetJson);

            IndexResponse ir = irb.get();
        
            // Index name
            String _index = ir.getIndex();
            // Type name
            String _type = ir.getType();
            // Document ID (generated or not)
            String _id = ir.getId();
            // Version (if it's the first time you index this document, you will get: 1)
            long _version = ir.getVersion();
            // isCreated() is true if the document is a new one, false if it has been updated
            boolean created = ir.isCreated();
            
            String outString = String.format("indexDocument(): _index = %s, _type = %s, _id = %s, _version =%d, created = %b", _index, _type, _id, _version, created);
            System.out.println(outString);
        
        } catch (JsonProcessingException ex) {
            java.util.logging.Logger.getLogger(ElasticsearchDemoApp.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(ElasticsearchDemoApp.class.getName()).log(Level.SEVERE, null, ex);
        }
 	esClient.shutdown();
    }

    private void getDocument() {
        GetResponse response = esClient.getClient().prepareGet("twitter", "tweet", "2").get();
        String outString = String.format("getDocument(): id = %s, fieldValue = %s", response.getId(), response.getSourceAsString());
        System.out.println(outString);        
    }
    
    private void deleteDocument() {
        DeleteResponse response = esClient.getClient().prepareDelete("twitter", "tweet", "2").get();
        String outString = String.format("deleteDocument(): id = %s", response.getId());
        System.out.println(outString);        
    }
    
    private void searchForDocument() {
            SearchResponse response = esClient.getClient().prepareSearch().execute().actionGet();
//            SearchResponse response = esClient.getClient().prepareSearch("twitter")
//                .setTypes("tweet")
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(QueryBuilders.termQuery("multi", "test"))                 // Query
//                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
//                .setFrom(0).setSize(60).setExplain(true)
//                .execute()
//                .actionGet();    
        
            String outString = String.format("totalHits(): = %s", response.getHits().totalHits());
            System.out.println(outString);        
    
    }

}
