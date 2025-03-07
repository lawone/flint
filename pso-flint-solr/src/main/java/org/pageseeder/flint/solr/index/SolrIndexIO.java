package org.pageseeder.flint.solr.index;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Date;
import java.util.ArrayList;
import java.util.Optional;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.NamedList;

import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.pageseeder.flint.Index;
import org.pageseeder.flint.IndexException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;
import org.pageseeder.flint.IndexIO;
import org.pageseeder.flint.content.DeleteRule;
import org.pageseeder.flint.indexing.FlintDocument;
import org.pageseeder.flint.solr.SolrCollectionManager;
import org.pageseeder.flint.solr.SolrFlintConfig;
import org.pageseeder.flint.solr.SolrFlintException;
import org.pageseeder.flint.solr.SolrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

public class SolrIndexIO implements IndexIO {

	private final static Logger LOGGER = LoggerFactory.getLogger(SolrIndexIO.class);

	private  SolrClient _client;

	private final String _collection;

	public SolrIndexIO(Index index) {
	    this._collection = index.getIndexID(); 

	    // Build client to connect to Solr
	    SolrFlintConfig config = SolrFlintConfig.getInstance();
	    Collection<String> zkhosts = config.getZKHosts();
	    
	    if (zkhosts != null && !zkhosts.isEmpty()) {
	        // SolrCloud Mode (Using ZooKeeper)
	        List<String> zkHostsList = new ArrayList<>(zkhosts);
	        LOGGER.info("Connection through solr cloud mode (Using ZooKeeper)");
	        try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(zkHostsList, Optional.empty()).build()) {
	            //cloudSolrClient.connect();

	            if (cloudSolrClient.getClusterStateProvider() instanceof ZkClientClusterStateProvider) {
	                ZkClientClusterStateProvider zkProvider =
	                        (ZkClientClusterStateProvider) cloudSolrClient.getClusterStateProvider();

	                //Ensure the selected collection has an active leader
	                ClusterState clusterState = zkProvider.getClusterState();
	                Slice slice = null;
	                String selectedCollection = null; //Temporary variable to store the collection
	                for (String collectionName : clusterState.getCollectionsMap().keySet()) {
	                    Slice tempSlice = clusterState.getCollection(collectionName).getActiveSlices().iterator().next();
	                    if (tempSlice.getLeader() != null) {
	                        slice = tempSlice;
	                        selectedCollection = collectionName;
	                        LOGGER.info("Auto-selected collection for leader node: " + selectedCollection);
	                        break;
	                    }
	                }

	             // If no collection with a leader is found, throw an error
	                if (selectedCollection == null || slice == null || slice.getLeader() == null) {
	                	LOGGER.error("No Solr collections with an active leader found.");
	                    throw new RuntimeException("No Solr collections with an active leader found.");
	                }

	                

	             // Get the leader node's base URL
	                String leaderBaseUrl = slice.getLeader().getBaseUrl();

	               
	               
	                if(leaderBaseUrl != null && leaderBaseUrl.endsWith("/")) {
	                	leaderBaseUrl=leaderBaseUrl.substring(0, leaderBaseUrl.length()-1);
	    	        }
	                LOGGER.info("URL for solr leader node: " + leaderBaseUrl);

	                //Create Http2SolrClient with correct timeout settings
	                Http2SolrClient http2SolrClient = new Http2SolrClient.Builder(leaderBaseUrl)
	                        .withConnectionTimeout(160, TimeUnit.SECONDS)
	                        .withRequestTimeout(60, TimeUnit.SECONDS)
	                        .build();

	                // Use ConcurrentUpdateHttp2SolrClient
	                this._client = new ConcurrentUpdateHttp2SolrClient.Builder(leaderBaseUrl, http2SolrClient)
	                        .withThreadCount(4)
	                        .withQueueSize(50)
	                        .build();

	               

	            } else {
	            	LOGGER.error("ClusterStateProvider is not an instance of ZkClientClusterStateProvider");
	                throw new RuntimeException("ClusterStateProvider is not an instance of ZkClientClusterStateProvider");
	            }
	        } catch (Exception e) {
	            throw new RuntimeException("Failed to get SolrCloud leader node", e);
	        }
	    } else {
	    	try {
	        // Standalone Solr Mode (No ZooKeeper)
	        String standaloneUrl = config.getServerURL() ; 
	        LOGGER.info("Connection through stand alone mode (No ZooKeeper)");
	        if(standaloneUrl != null && standaloneUrl.endsWith("/")) {
	        	standaloneUrl=standaloneUrl.substring(0, standaloneUrl.length()-1);
	        }
	        LOGGER.info("Final standalone Solr URL: " + standaloneUrl);
	        // Create Http2SolrClient with correct timeout settings
	        Http2SolrClient http2SolrClient = new Http2SolrClient.Builder(standaloneUrl)
	                .withConnectionTimeout(160, TimeUnit.SECONDS)
	                .withRequestTimeout(60, TimeUnit.SECONDS)
	                .build();

	        // Use ConcurrentUpdateHttp2SolrClient
	        this._client = new ConcurrentUpdateHttp2SolrClient.Builder(standaloneUrl, http2SolrClient)
	                .withThreadCount(4)
	                .withQueueSize(50)
	                .build();

	    	}catch(Exception e) {
	    		LOGGER.error("Error when making connection through stand alone mode (No ZooKeeper)", e);
	    		throw new RuntimeException("Error when making connection through stand alone mode (No ZooKeeper) : ", e);
	    	}
	        
	    }
	}

public void start() throws SolrFlintException {
	start(null);
}

public void start(Map<String, String> attributes) throws SolrFlintException {
	// make sure it exists on solr server
	if (!new SolrCollectionManager().createCollection(this._collection, attributes))
		throw new SolrFlintException("Failed to create collection "+this._collection, null);
}

@Override
public long getLastTimeUsed() {
	long lastModifiedTime = -1;
	LukeRequest request = new LukeRequest();
	request.setShowSchema(true);

	try {
		LukeResponse response = request.process(this._client, this._collection);
		if (response != null) {

			Date lastModified = (Date)response.getIndexInfo().get("lastModified");
			if(lastModified != null){
				lastModifiedTime =lastModified.getTime();
			}else{
				return lastModifiedTime; 
			}
		}

	} catch ( SolrServerException | IOException | NullPointerException ex) {
		LOGGER.error("Cannot get last modified time ", ex);
	}

	return lastModifiedTime;
}

@Override
public void stop() throws IndexException {
	// nothing to do here?
	try {
		this._client.close();
	} catch (IOException ex) {
		LOGGER.error("Failed to close index!", ex);
	}
}

@Override
public void maybeRefresh() {
	// reset
}

@Override
public void maybeCommit() {
	try {
		// commit
		this._client.commit(this._collection);
	} catch (SolrServerException | IOException ex) {
		LOGGER.error("Failed to commit index!", ex);
	}
}

@Override
public boolean clearIndex() throws IndexException {
	UpdateResponse resp;
	try {
		resp = this._client.deleteByQuery(this._collection, "*:*");
		resp = this._client.commit(this._collection);
	} catch (SolrServerException | IOException ex) {
		throw new IndexException("Failed to clear index", ex);
	}
	return resp.getStatus() == 0;
}

@Override
public boolean deleteDocuments(DeleteRule rule) throws IndexException {
	if (rule instanceof SolrDeleteRule) {
		SolrDeleteRule drule = (SolrDeleteRule) rule;
		try {
			UpdateResponse resp;
			if (drule.deleteByID()) {
				resp = this._client.deleteById(this._collection, drule.getDeleteID());
			} else {
				resp = this._client.deleteByQuery(this._collection, drule.getDeleteQuery());
			}
			return resp.getStatus() == 0;
		} catch (SolrServerException | IOException ex) {
			throw new IndexException("Failed to delete document(s)", ex);
		}
	}
	return false;
}

@Override
public boolean updateDocuments(DeleteRule rule, List<FlintDocument> documents) throws IndexException {
	
	// delete first
	if (rule != null) deleteDocuments(rule);
	// add then
	try {
		UpdateResponse resp = this._client.add(this._collection, SolrUtils.toDocuments(documents));
		return resp.getStatus() == 0;
		
	} catch (SolrServerException | IOException ex) {
		throw new IndexException("Failed to add document(s)", ex);
	}
}

public QueryResponse query(SolrQuery query) {
	try {
		return this._client.query(this._collection, query);
	} catch (SolrServerException | IOException ex) {
		LOGGER.error("Failed to run solr query {}", query, ex);
	}
	return null;
}

public QueryResponse query(SolrParams params) {
	try {
		return this._client.query(this._collection, params);
	} catch (SolrServerException | IOException ex) {
		LOGGER.error("Failed to run query with params {}", params, ex);
	}
	return null;
}

public QueryResponse request(QueryRequest request) {
	try {
		return request.process(this._client, this._collection);
	} catch (SolrServerException | IOException ex) {
		LOGGER.error("Failed to run query request {}", request, ex);
	}
	return null;
}

public SolrResponse process(SolrRequest<? extends SolrResponse> request) {
	
	try {
		return request.process(this._client, this._collection);
	} catch (SolrServerException | IOException ex) {
		LOGGER.error("Failed to process request {}", request, ex);
	}
	return null;
}

public SolrClient getClient() {
	return this._client;
}

public String getCollection() {
	return this._collection;
}


}
