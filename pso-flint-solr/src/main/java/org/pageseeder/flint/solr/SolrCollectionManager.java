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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.util.NamedList;
import org.pageseeder.xmlwriter.XML.NamespaceAware;
import org.pageseeder.xmlwriter.XMLStringWriter;
import org.pageseeder.xmlwriter.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final int defaultShards;

  private final int defaultReplicas;

  public SolrCollectionManager() {
    // build client to connect to solr
    SolrFlintConfig config = SolrFlintConfig.getInstance();
    // use cloud?
    List<String> zkhosts = (List<String>) config.getZKHosts();
    if (zkhosts != null && !zkhosts.isEmpty()) {
      this.defaultShards = zkhosts.size();
      this.defaultReplicas = 1;
     // this._solr = new CloudSolrClient.Builder().withZkHost(zkhosts).build();
      this._solr = new CloudSolrClient.Builder(zkhosts).build();
    } else {
      this.defaultShards = 1;
      this.defaultReplicas = 1;
      this._solr = new HttpSolrClient.Builder(config.getServerURL()).allowCompression(true).build();
    }
  }

  public SolrCollectionManager(Collection<String> zkhosts) {
    this.defaultShards = zkhosts.size();
    this.defaultReplicas = 1;
    this._solr = new CloudSolrClient.Builder((List<String>) zkhosts).build();;
  }

  public SolrCollectionManager(String url) {
    this.defaultShards = 1;
    this.defaultReplicas = 1;
    this._solr = new HttpSolrClient.Builder(url).allowCompression(true).build();
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
