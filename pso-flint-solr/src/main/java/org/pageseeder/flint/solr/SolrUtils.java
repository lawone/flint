package org.pageseeder.flint.solr;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.common.SolrInputDocument;
import org.pageseeder.flint.indexing.FlintDocument;
import org.pageseeder.flint.indexing.FlintField;

public class SolrUtils {

  public static Collection<SolrInputDocument> toDocuments(Collection<FlintDocument> docs) {
    Collection<SolrInputDocument> sdocs = new ArrayList<>();
    for (FlintDocument fdoc : docs) {
      SolrInputDocument sdoc = new SolrInputDocument();
      for (FlintField field : fdoc.fields()) {
       // sdoc.addField(field.name(), field.value(), field.boost());
        sdoc.addField(field.name(), field.value());
      }
      sdocs.add(sdoc);
    }
    return sdocs;
  }

}
