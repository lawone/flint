/*
 * Copyright (c) 1999-2016 Allette systems pty. ltd.
 */
package org.pageseeder.flint.solr;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.NamedList;
import org.pageseeder.xmlwriter.XML.NamespaceAware;
import org.pageseeder.xmlwriter.XMLStringWriter;
import org.pageseeder.xmlwriter.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.response.SolrPingResponse;


/**
 * A server level manager to deal with the collections.
 *
 * @author Jean-Baptiste Reure
 * @since 16 May 2017
 */
public class SolrCollectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrCollectionManager.class);

  private static final int SUCCESS_STATUS = 0;

  public static final String SHARDS = "shards";

  public static final String NUM_SHARDS = "num.shards";

  public static final String NUM_REPLICAS = "num.replicas";

  public static final String ROUTER_NAME = "router.name";

  public static final String ROUTER_FIELD = "router.field";

  public static final String MAX_SHARDS_PER_NODE = "max.shards.per.node";

  private final SolrClient _solr;
  private String defaultCollection;
  private final int defaultShards;

  private final int defaultReplicas;

	
  public SolrCollectionManager() {
	    // Build client to connect to Solr
	    SolrFlintConfig config = SolrFlintConfig.getInstance();
	    Collection<String> zkhosts = config.getZKHosts();

	    if (zkhosts != null && !zkhosts.isEmpty()) {
	        // SolrCloud Mode (Using ZooKeeper)
	    	LOGGER.info("Connection through solr cloud mode (Using ZooKeeper)");
	        List<String> zkHostsList = new ArrayList<>(zkhosts);
	        this.defaultShards = zkHostsList.size();
	        this.defaultReplicas = 1;

	        try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(zkHostsList, Optional.empty()).build()) {
	            //cloudSolrClient.connect();

	            if (cloudSolrClient.getClusterStateProvider() instanceof ZkClientClusterStateProvider) {
	                ZkClientClusterStateProvider zkProvider =
	                        (ZkClientClusterStateProvider) cloudSolrClient.getClusterStateProvider();

	                // Get a valid collection with an active leader
	                ClusterState clusterState = zkProvider.getClusterState();
	                Slice slice = null;
	                for (String collectionName : clusterState.getCollectionsMap().keySet()) {
	                   slice = clusterState.getCollection(collectionName).getActiveSlices().iterator().next();
	                    if (slice.getLeader() != null) { // Ensure collection has a leader
	                        this.defaultCollection = collectionName;
	                        LOGGER.info("Auto-selected collection for leader node: " + this.defaultCollection);
	                        break;
	                    }
	                }

	                // If no collection with a leader is found, throw an error
	                if (this.defaultCollection == null) {
	                	LOGGER.error("No Solr collections with an active leader found.");
	                    throw new RuntimeException("No Solr collections with an active leader found.");
	                }

	             // Get the leader node's base URL
	                String leaderBaseUrl = slice.getLeader().getBaseUrl();

	                if(leaderBaseUrl != null && leaderBaseUrl.endsWith("/")) {
	                	leaderBaseUrl=leaderBaseUrl.substring(0, leaderBaseUrl.length()-1);
	    	        }
					
	                LOGGER.info("URL for solr leader node: " + leaderBaseUrl);
	                // Create Http2SolrClient
	                Http2SolrClient http2SolrClient = new Http2SolrClient.Builder(leaderBaseUrl)
	                        .withConnectionTimeout(160, TimeUnit.SECONDS)
	                        .withRequestTimeout(60, TimeUnit.SECONDS)//No socketTimeout method. Need to handle through this property
	                        .build();
	             

	                // Use ConcurrentUpdateHttp2SolrClient
	                this._solr = new ConcurrentUpdateHttp2SolrClient.Builder(leaderBaseUrl, http2SolrClient)
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
	        //  Standalone Solr Mode (No ZooKeeper provided in config file)
	    	LOGGER.info("Connection through stand alone mode (No ZooKeeper)");
	        this.defaultShards = 1;
	        this.defaultReplicas = 1;
	        String standaloneUrl = config.getServerURL() ;
	      
	        if(standaloneUrl != null && standaloneUrl.endsWith("/")) {
	        	standaloneUrl=standaloneUrl.substring(0, standaloneUrl.length()-1);
	        }
	        LOGGER.info("Final standalone Solr URL: " + standaloneUrl);

	        //  Create Http2SolrClient
	        Http2SolrClient http2SolrClient = new Http2SolrClient.Builder(standaloneUrl)
	                .withConnectionTimeout(160, TimeUnit.SECONDS)
	                .withRequestTimeout(60, TimeUnit.SECONDS)
	                .build();

	        //  Pass Http2SolrClient to ConcurrentUpdateHttp2SolrClient
	        this._solr = new ConcurrentUpdateHttp2SolrClient.Builder(standaloneUrl, http2SolrClient)
	                .withThreadCount(4)
	                .withQueueSize(50)
	                .build();
	        
	       
	        } catch (Exception e) {
	        	LOGGER.error("Error when making connection through stand alone mode (No ZooKeeper)",e);
	            throw new RuntimeException("Error when making connection through stand alone mode (No ZooKeeper) : ", e);
	        }
	    }
	}
	
  
  public SolrCollectionManager(Collection<String> zkhosts) {
	    if (zkhosts == null || zkhosts.isEmpty()) {
	        throw new IllegalArgumentException("ZooKeeper hosts cannot be null or empty");
	    }
	   
	    //SolrCloud Mode (Using ZooKeeper)
	    List<String> zkHostsList = new ArrayList<>(zkhosts);
	    this.defaultShards = zkHostsList.size();
	    this.defaultReplicas = 1;

	    try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(zkHostsList, Optional.empty()).build()) {
	       //cloudSolrClient.connect();

	        if (cloudSolrClient.getClusterStateProvider() instanceof ZkClientClusterStateProvider) {
	            ZkClientClusterStateProvider zkProvider =
	                    (ZkClientClusterStateProvider) cloudSolrClient.getClusterStateProvider();

	            // Get a valid collection with an active leader
	            ClusterState clusterState = zkProvider.getClusterState();
	            Slice slice = null;
	            for (String collectionName : clusterState.getCollectionsMap().keySet()) {
	                slice = clusterState.getCollection(collectionName).getActiveSlices().iterator().next();
	                if (slice.getLeader() != null) {
	                    this.defaultCollection = collectionName;
	                    LOGGER.info("Auto-selected collection for leader node: " + this.defaultCollection);
	                    break;
	                }
	            }

	            if (this.defaultCollection == null) {
	            	LOGGER.error("No Solr collections with an active leader found.");
	                throw new RuntimeException("No Solr collections with an active leader found.");
	            }

	         // Get the leader node's base URL
	            String leaderBaseUrl = slice.getLeader().getBaseUrl();

	            LOGGER.info("Final Leader Solr URL: " + leaderBaseUrl);

	            // Create Http2SolrClient
	            Http2SolrClient http2SolrClient = new Http2SolrClient.Builder(leaderBaseUrl)
	                    .withConnectionTimeout(160, TimeUnit.SECONDS)
	                    .withRequestTimeout(60, TimeUnit.SECONDS)
	                    .build();

	            //Use ConcurrentUpdateHttp2SolrClient
	            this._solr = new ConcurrentUpdateHttp2SolrClient.Builder(leaderBaseUrl, http2SolrClient)
	                    .withThreadCount(4)
	                    .withQueueSize(50)
	                    .build();
	        } else {
	        	LOGGER.error("ClusterStateProvider is not an instance of ZkClientClusterStateProvider");
	            throw new RuntimeException("ClusterStateProvider is not an instance of ZkClientClusterStateProvider");
	        }
	    } catch (Exception e) {
	    	LOGGER.error("Failed to get SolrCloud leader node", e);
	        throw new RuntimeException("Failed to get SolrCloud leader node", e);
	    }
	}

	

  public SolrCollectionManager(String url) {
	    if (url == null || url.isEmpty()) {
	        throw new IllegalArgumentException("Solr URL cannot be null or empty");
	    }

	    this.defaultShards = 1;
	    this.defaultReplicas = 1;


	    // Create Http2SolrClient
	    Http2SolrClient http2SolrClient = new Http2SolrClient.Builder(url)
	            .withConnectionTimeout(160, TimeUnit.SECONDS)
	            .withRequestTimeout(60, TimeUnit.SECONDS)
	            .build();

	    // Use ConcurrentUpdateHttp2SolrClient
	    this._solr = new ConcurrentUpdateHttp2SolrClient.Builder(url, http2SolrClient)
	            .withThreadCount(4)
	            .withQueueSize(50)
	            .build();
	}
  
  
  @SuppressWarnings("unchecked")
  public Collection<String> listCollections() throws SolrFlintException {
    CollectionAdminResponse response = null;
    try {
      CollectionAdminRequest.List req = new CollectionAdminRequest.List();
      response = req.process(this._solr);
    } catch (  IOException | SolrServerException ex) {
      if (ex.getCause() != null && ex.getCause() instanceof ConnectException) throw new SolrFlintException(true);
      throw new SolrFlintException("Failed to list Solr collections", ex);
    }
    if (response == null) return Collections.emptyList();
    return (ArrayList<String>) response.getResponse().get("collections");
  }

  @SuppressWarnings("unchecked")
  public ClusterStatus getClusterStatus() throws SolrFlintException {
    CollectionAdminResponse response = null;
    try {
      CollectionAdminRequest.ClusterStatus req = CollectionAdminRequest.getClusterStatus();
      response = req.process(this._solr);
    } catch ( SolrServerException | IOException ex) {
      if (ex.getCause() != null && ex.getCause() instanceof ConnectException) throw new SolrFlintException(true);
      throw new SolrFlintException("Failed to list Solr collections", ex);
    }
    if (response == null) return null;
    return ClusterStatus.fromNamedList((NamedList<Object>) response.getResponse().get("cluster"));
  }

  /**
   * Create a new collection with the default router ()
   * @param name the name of core.
   * @return the status of creation
   */
  public boolean createCollection(String name) throws SolrFlintException {
    return createCollection(name, null);
  }

  /**
   * According to the document https://cwiki.apache.org/confluence/display/solr/CoreAdmin+API,
   * The SOLR core creation is not a proper API.
   * To create a core, you can either use the command 'solr create -c [name]'
   * or use the configSet (current implementation).
   *
   * @param name the name of core.
   * @return the status of creation
   */
  public boolean createCollection(String name, Map<String, String> attributes) throws SolrFlintException {
    CollectionAdminResponse response = null;

    // check core exist
    if (!exists(name)) {
      // make sure config set exists
      LOGGER.info("Solr collection {} - creating", name);
      Map<String, String> atts = attributes == null ? Collections.emptyMap() : new HashMap<>(attributes);
      try {
        // find all attributes
        int shards;
        int replicas;
        String s = atts.remove(NUM_SHARDS);
        try {
          shards = s == null ? this.defaultShards : Integer.parseInt(s);
        } catch (NumberFormatException ex) {
          LOGGER.error("Ignoring invalid number of shards {} for collection {}", s, name);
          shards = this.defaultShards;
        }
        String r = atts.remove(NUM_REPLICAS);
        try {
          replicas = r == null ? this.defaultReplicas : Integer.parseInt(r);
        } catch (NumberFormatException ex) {
          LOGGER.error("Ignoring invalid number of replicas {} for collection {}", r, name);
          replicas = this.defaultReplicas;
        }
        CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(name,
            shards <= 0 ? this.defaultShards : shards,
            replicas <= 0 ? this.defaultReplicas : replicas);
        if (atts.containsKey(ROUTER_NAME))  create.setRouterName(atts.remove(ROUTER_NAME));
        if (atts.containsKey(ROUTER_FIELD)) create.setRouterField(atts.remove(ROUTER_FIELD));
        if (atts.containsKey(SHARDS))       create.setShards(atts.remove(SHARDS));
        if (atts.containsKey(MAX_SHARDS_PER_NODE)) {
          String m = atts.remove(MAX_SHARDS_PER_NODE);
          try {
            create.setShards(m);
          } catch (NumberFormatException ex) {
            LOGGER.error("Ignoring invalid max shards per node {} for collection {}", m, name);
          }
        }
        if (!atts.isEmpty()) {
          LOGGER.warn("Ignoring non supported attributes {} when creating collection {}", atts.keySet().toArray().toString(), name);
        }
        // ok create it
        response = create.process(this._solr);
      } catch (RemoteSolrException | SolrServerException | IOException ex) {
        LOGGER.error("Cannot create collection {}", name, ex);
        if (ex.getCause() != null && ex.getCause() instanceof ConnectException) throw new SolrFlintException(true);
        throw new SolrFlintException("Failed to create collection "+name+": "+ex.getMessage(), ex);
      }
    } else {
      LOGGER.info("Solr collection {} already exists - reloading it", name);
      try {
        response = CollectionAdminRequest.reloadCollection(name).process(this._solr);
      } catch (RemoteSolrException | SolrServerException | IOException ex) {
        if (ex.getCause() != null && ex.getCause() instanceof ConnectException) throw new SolrFlintException(true);
        throw new SolrFlintException("Failed to reload collection " + name+": "+ex.getMessage(), ex);
      }
    }
    return response != null && response.getStatus() == SUCCESS_STATUS;
  }

  /**
   * @param name the name of core
   * @return the status whether it exists
   * @throws SolrFlintException 
   */
  public boolean deleteCollection(String name) throws SolrFlintException {
    CollectionAdminResponse response = null;
    try {
      response = CollectionAdminRequest.deleteCollection(name).process(this._solr);
    } catch (RemoteSolrException | SolrServerException | IOException ex) {
      if (ex.getCause() != null && ex.getCause() instanceof ConnectException) throw new SolrFlintException(true);
      throw new SolrFlintException("Failed to delete collection " + name, ex);
    }
    return response.getResponse().get("success") != null;
  }

  /**
   * @param name the name of core
   * @return the status whether it exists
   * @throws SolrFlintException 
   */
  public boolean exists(String name) throws SolrFlintException {
    Collection<String> existing = listCollections();
    return existing != null && existing.contains(name);
  }

  public static void main(String[] args) throws SolrFlintException, IOException {
    XMLWriter xml = new XMLStringWriter(NamespaceAware.No);
    ClusterStatus status = new SolrCollectionManager("http://localhost:8983/solr").getClusterStatus();
    if (status != null) status.toXML(xml);
    System.out.println(xml.toString());
  }
}
