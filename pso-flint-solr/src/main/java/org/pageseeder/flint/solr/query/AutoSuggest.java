package org.pageseeder.flint.solr.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.pageseeder.flint.Index;
import org.pageseeder.flint.solr.index.SolrIndexIO;
import org.apache.solr.common.params.ModifiableSolrParams;
public class AutoSuggest {

  private final SolrIndexIO _indexio;

  private final String _name;
  
  private Collection<String> _dictionaries = new ArrayList<>();

  private long lastBuilt = -1;

  public AutoSuggest(Index index, String name) {
    this(index, name, null);
  }

  public AutoSuggest(Index index, String name, Collection<String> dictionaries) {
    this._indexio = (SolrIndexIO) index.getIndexIO();
    this._name = name;
    if (dictionaries == null) this._dictionaries.add(name);
    else this._dictionaries.addAll(dictionaries);
  }

  public String getName() {
    return this._name;
  }

  public boolean isCurrent() {
    return this.lastBuilt > 0 && this._indexio.getLastTimeUsed() < this.lastBuilt;
  }

  public List<Suggestion> suggest(String text) {
    return suggest(text, 10);
  }

  public List<Suggestion> suggest(String text, int nb) {
    return suggest(text, null, nb);
  }

  public List<Suggestion> suggest(String text, String with, int nb) {
    List<Suggestion> suggestions = new ArrayList<>();
    // build params
    NamedList<String> params = new NamedList<>();
    params.add("suggest",            "true");
    for (String dic : this._dictionaries)
      params.add("suggest.dictionary", dic);
    params.add("suggest.q",          text);
    params.add("suggest.count",      String.valueOf(nb));
    params.add("suggest.build",      String.valueOf(!isCurrent()));
    if (with != null) {
      params.add("suggest.cfq",      with);
    }
    
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    for (Map.Entry<String, String> entry : params) {
        solrParams.set(entry.getKey(), entry.getValue());
    }
    // build request
    GenericSolrRequest suggest = new GenericSolrRequest(METHOD.GET, "/suggest", solrParams);
    SolrResponse response = this._indexio.process(suggest);
    
    if (response != null && response.getResponse() != null) {
      this.lastBuilt = System.currentTimeMillis();
      Object suggested = response.getResponse().get("suggest");
      if (suggested != null && suggested instanceof Map) {
        Map<String, Object> suggesters = (Map<String, Object>) suggested;
        for (String name : suggesters.keySet()) {
          Collection<NamedList<Object>> list = (Collection<NamedList<Object>>) ((NamedList<Object>) ((NamedList<Object>) suggesters.get(name)).getVal(0)).get("suggestions");
          if (list != null) {
            for (NamedList<Object> result : list) {
              Suggestion suggestion = new Suggestion();
              suggestion.text    = (String) result.get("term");
              suggestion.weight  = (Long)   result.get("weight");
              suggestion.payload = (String) result.get("payload");
              if (!suggestions.contains(suggestion))
                suggestions.add(suggestion);
            }
          }
        }
      }
    }
    return suggestions;
  }

  public static class Suggestion {
    public String text;
    public String payload;
    public long weight;
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Suggestion)) return false;
      Suggestion s = (Suggestion) obj;
      return this.text.equals(s.text) && ((this.payload == null && s.payload == null) || this.payload.equals(s.payload));
    }
    @Override
    public int hashCode() {
      return this.text.hashCode() * 3 + (this.payload != null ? 17 * this.payload.hashCode() : 0);
    }
    @Override
    public String toString() {
      return this.text + (weight != 100 ? "("+weight+")" : "");
    }
  }
}
