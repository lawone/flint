package org.pageseeder.flint.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.NamedList;
import org.pageseeder.xmlwriter.XMLWritable;
import org.pageseeder.xmlwriter.XMLWriter;

public class ClusterStatus implements XMLWritable {

  private List<Collection> collections = new ArrayList<>();
  private List<String> liveNodes = new ArrayList<>();

  @SuppressWarnings("unchecked")
  public static ClusterStatus fromNamedList(NamedList<Object> list) {
    ClusterStatus status = new ClusterStatus();
    NamedList<Object> cols = (NamedList<Object>) list.get("collections");
    if (cols != null) for (int i = 0; i < cols.size(); i++) {
      status.collections.add(Collection.fromMap(cols.getName(i), (Map<String, Object>) cols.getVal(i)));
    }
    List<Object> nodes = (List<Object>) list.get("live_nodes");
    if (nodes != null) for (Object node : nodes) {
      status.liveNodes.add(node.toString());
    }
    return status;
  }

  @Override
  public void toXML(XMLWriter xml) throws IOException {
    xml.openElement("cluster");
    xml.openElement("collections");
    for (Collection c : this.collections) c.toXML(xml);
    xml.closeElement();
    xml.openElement("live-nodes");
    for (String ln : this.liveNodes) {
      xml.openElement("live-node");
      xml.attribute("url", ln);
      xml.closeElement();
    }
    xml.closeElement();
    xml.closeElement();
  }

  private final static class Collection implements XMLWritable {
    private String name;
    private String routerName;
    private String routerField;
    private int replicationFactor = -1;
    private int maxShardsPerNode = -1;
    private boolean autoAddReplicas;
    private List<Shard> shards = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public static Collection fromMap(String aname, Map<String, Object> col) {
      Collection created = new Collection();
      if (col == null || aname == null) return created;
      created.name = aname;
      created.autoAddReplicas   = "true".equals(col.get("autoAddReplicas"));
      created.maxShardsPerNode  = col.get("maxShardsPerNode")  == null ? -1 : Integer.parseInt(String.valueOf(col.get("maxShardsPerNode")));
      created.replicationFactor = col.get("replicationFactor") == null ? -1 : Integer.parseInt(String.valueOf(col.get("replicationFactor")));
      created.routerName  = col.get("router") == null ? null : (String) ((Map<String, Object>) col.get("router")).get("name");
      created.routerField = col.get("router") == null ? null : (String) ((Map<String, Object>) col.get("router")).get("field");
      Map<String, Object> theshards = (Map<String, Object>) col.get("shards");
      if (theshards != null) for (String name : theshards.keySet()) {
        created.shards.add(Shard.fromMap(name, (Map<String, Object>) theshards.get(name)));
      }
      return created;
    }

    @Override
    public void toXML(XMLWriter xml) throws IOException {
      xml.openElement("collection");
      attribute("name", this.name, xml);
      attribute("router-name", this.routerName, xml);
      attribute("router-field", this.routerField, xml);
      if (this.replicationFactor >= 0) xml.attribute("replication-factor", this.replicationFactor);
      if (this.maxShardsPerNode  >= 0) xml.attribute("max-shards-per-node", this.maxShardsPerNode);
      xml.attribute("auto-add-replicas", this.autoAddReplicas ? "true" : "false");
      for (Shard s : this.shards) s.toXML(xml);
      xml.closeElement();
    }
  }

  private final static class Shard implements XMLWritable {
    private String name;
    private String state;
    private List<Replica> replicas = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public static Shard fromMap(String aname, Map<String, Object> col) {
      Shard created = new Shard();
      created.name  = aname;
      created.state = (String) col.get("state");
      Map<String, Object> thereplicas = (Map<String, Object>) col.get("replicas");
      if (thereplicas != null) for (String name : thereplicas.keySet()) {
        created.replicas.add(Replica.fromMap(name, (Map<String, Object>) thereplicas.get(name)));
      }
      return created;
    }

    @Override
    public void toXML(XMLWriter xml) throws IOException {
      xml.openElement("shard");
      attribute("name", this.name, xml);
      attribute("state", this.state, xml);
      for (Replica r : this.replicas) r.toXML(xml);
      xml.closeElement();
    }
  }

  private final static class Replica implements XMLWritable {
    private String name;
    private String core;
    private String url;
    private String nodeName;
    private String state;
    private boolean leader;

    public static Replica fromMap(String aname, Map<String, Object> col) {
      Replica created = new Replica();
      created.name     = aname;
      created.core     = (String) col.get("core");
      created.url      = (String) col.get("base_url");
      created.nodeName = (String) col.get("node_name");
      created.state    = (String) col.get("state");
      created.leader   = ("true").equals(col.get("leader"));
      return created;
    }

    @Override
    public void toXML(XMLWriter xml) throws IOException {
      xml.openElement("replica");
      attribute("name", this.name, xml);
      attribute("core", this.core, xml);
      attribute("url", this.url, xml);
      attribute("node-name", this.nodeName, xml);
      attribute("state", this.state, xml);
      attribute("leader", this.leader ? "true" : "false", xml);
      xml.closeElement();
    }
  }

  private static void attribute(String name, String value, XMLWriter xml) throws IOException {
    if (name != null && value != null && !name.isEmpty() && !value.isEmpty())
      xml.attribute(name, value);
  }
}
