package org.pageseeder.flint.berlioz.model;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.TransformerException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.pageseeder.berlioz.util.FileUtils;
import org.pageseeder.berlioz.util.MD5;
import org.pageseeder.flint.IndexException;
import org.pageseeder.flint.IndexManager;
import org.pageseeder.flint.Requester;
import org.pageseeder.flint.indexing.FlintDocument;
import org.pageseeder.flint.indexing.IndexJob;
import org.pageseeder.flint.local.LocalFileContent;
import org.pageseeder.flint.lucene.LuceneIndexQueries;
import org.pageseeder.flint.lucene.LuceneLocalIndex;
import org.pageseeder.flint.lucene.query.SearchPaging;
import org.pageseeder.flint.lucene.query.SearchQuery;
import org.pageseeder.flint.lucene.query.SearchResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public final class IndexMaster {

  /**
   * private logger
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexMaster.class);
  private final IndexManager _manager;
  private final String _name;
  private final FileFilter _indexingFileFilter;
  private final File _indexRoot;
  private final File _contentRoot;
  private final LuceneLocalIndex _index;
  private final IndexDefinition _def;

  private final Map<String, org.pageseeder.flint.lucene.search.AutoSuggest> _autosuggests = new HashMap<>();
  
  public static IndexMaster create(IndexManager mgr, String name,
         File content, File index, IndexDefinition def) throws TransformerException {
    return create(mgr, name, content, index, "psml", def);
  }

  public static IndexMaster create(IndexManager mgr, String name,
         File content, File index, String extension, IndexDefinition def) throws TransformerException {
    return new IndexMaster(mgr, name, content, index, extension, def);
  }

  private IndexMaster(IndexManager mgr, String name, File content,
      File index, String extension, IndexDefinition def) throws TransformerException {
    this._manager = mgr;
    this._name = name;
    this._contentRoot = content;
    this._indexRoot = index;
    this._index = new LuceneLocalIndex(this._indexRoot, def.getName(), FlintConfig.newAnalyzer(), this._contentRoot);
    this._index.setTemplate(extension, def.getTemplate().toURI());
    this._def = def;
    this._indexingFileFilter = def.buildFileFilter(this._contentRoot);
    // create autosuggests
    for (String an : this._def.listAutoSuggestNames()) {
      getAutoSuggest(an);
    }
  }

  public void reloadTemplate() throws TransformerException {
    reloadTemplate("psml");
  }

  public void reloadTemplate(String extension) throws TransformerException {
    this._index.setTemplate(extension, this._def.getTemplate().toURI());
  }

  public boolean isInIndex(File file) {
    return FileUtils.contains(_contentRoot, file);
  }

  public LuceneLocalIndex getIndex() {
    return this._index;
  }

  public String getName() {
    return this._name;
  }

  /**
   * Use definition name as catalog (one catalog per definition).
   * @return the catalog name
   */
  public String getCatalog() {
    return this._def.getName();
  }

  public void clear() {
    Requester requester = new Requester("clear berlioz index");
    this._manager.clear(this._index, requester, IndexJob.Priority.HIGH);
  }

  public SearchResults query(SearchQuery query) throws IndexException {
    return LuceneIndexQueries.query(this._index, query);
  }

  public SearchResults query(SearchQuery query, SearchPaging paging) throws IndexException {
    return LuceneIndexQueries.query(this._index, query, paging);
  }

  public long lastModified() {
    return this._index.getIndexIO().getLastTimeUsed();
  }

  public IndexDefinition getIndexDefinition() {
    return this._def;
  }

  public FileFilter getIndexingFileFilter() {
    return this._indexingFileFilter;
  }

  public org.pageseeder.flint.lucene.search.AutoSuggest getAutoSuggest(List<String> fields, boolean terms, int min, List<String> resultFields, Map<String, Float> weights) {
    String name = createAutoSuggestTempName(fields, terms, min, resultFields);
    // block the list of suggesters so another thread doesn't try to create the same one
    synchronized (this._autosuggests) {
      org.pageseeder.flint.lucene.search.AutoSuggest existing = this._autosuggests.get(name);
      // create?
      if (existing == null || !existing.isCurrent(this._manager)) {
        try {
          if (existing != null) clearAutoSuggest(name, existing);
          return createAutoSuggest(name, terms, fields, min, resultFields, weights);
        } catch (IndexException | IOException ex) {
          LOGGER.error("Failed to create autosuggest {}", name, ex);
          return null;
        }
      }
      return existing;
    }
  }

  public org.pageseeder.flint.lucene.search.AutoSuggest getAutoSuggest(String name) {
    // block the list of suggesters so another thread doesn't try to create the same one
    synchronized (this._autosuggests) {
      org.pageseeder.flint.lucene.search.AutoSuggest existing = this._autosuggests.get(name);
      // create?
      if (existing == null || !existing.isCurrent(this._manager)) {
        IndexDefinition.AutoSuggestDefinition asd = this._def.getAutoSuggest(name);
        if (asd != null) {
          try {
            if (existing != null) clearAutoSuggest(name, existing);
            return createAutoSuggest(name, asd.useTerms(), asd.getSearchFields(),
                asd.minChars(), asd.getResultFields(), asd.getWeights());
          } catch (IndexException | IOException ex) {
            LOGGER.error("Failed to create autosuggest {}", name, ex);
            return null;
          }
        } else if (asd == null) {
          LOGGER.error("Failed to find autosuggest definition with name {}", name);
          return null;
        }
      }
      return existing;
    }
  }

  public static Query toQuery(String predicate) throws IndexException {
    QueryParser parser = new QueryParser("type", FlintConfig.newAnalyzer());
    Query condition = null;
    if (predicate != null && !"".equals(predicate)) {
      try {
        condition = parser.parse(predicate);
      } catch (ParseException ex) {
        throw new IndexException("Condition for the suggestion could not be parsed.", ex);
      }
    }
    return condition;
  }

  public IndexReader grabReader() throws IndexException {
    return LuceneIndexQueries.grabReader(this._index);
  }

  public IndexSearcher grabSearcher() throws IndexException {
    return LuceneIndexQueries.grabSearcher(this._index);
  }

  public void releaseSilently(IndexReader reader) {
    LuceneIndexQueries.releaseQuietly(this._index, reader);
  }

  public void releaseSilently(IndexSearcher searcher) {
    LuceneIndexQueries.releaseQuietly(this._index, searcher);
  }

  public void close() {
    this._index.close();
    // close autosuggests
    for (String name : this._autosuggests.keySet()) {
      clearAutoSuggest(name, this._autosuggests.get(name));
    }
  }

  public void generateIXML(File f, Writer out) throws IndexException, IOException {
    // create content
    LocalFileContent content = new LocalFileContent(f, null);
    this._manager.contentToIXML(this._index, content, this._index.getParameters(f), out);
  }

  public List<FlintDocument> generateLuceneDocuments(File f) throws IndexException, IOException {
    // create content
    LocalFileContent content = new LocalFileContent(f, null);
    return this._manager.contentToDocuments(this._index, content, this._index.getParameters(f));
  }

  // -------------------------------------------------------------------------------
  // autosuggest methods
  // -------------------------------------------------------------------------------

  private static String createAutoSuggestTempName(Collection<String> fields, boolean terms, int min, Collection<String> resultFields) {
    StringBuilder name = new StringBuilder();
    if (fields != null) {
      for (String field : fields)
        name.append(field).append('%');
    }
    name.append(terms).append('%');
    name.append(min).append('%');
    if (resultFields != null) {
      for (String field : resultFields)
        name.append(field).append('%');
    }
    return MD5.hash(name.toString()).toLowerCase();
  }

  private org.pageseeder.flint.lucene.search.AutoSuggest createAutoSuggest(String name, boolean terms, Collection<String> fields,
      int min, Collection<String> resultFields, Map<String, Float> weights) throws IndexException, IOException {
    // build name
    String autosuggestIndexName = this._name+"_"+name+"_autosuggest";
    // create folder
    File autosuggestIndex = new File(FlintConfig.get().getRootDirectory(), autosuggestIndexName);
    // create lucene dir
    Directory autosuggestDir = FSDirectory.open(autosuggestIndex.toPath());
    // create auto suggest object
    org.pageseeder.flint.lucene.search.AutoSuggest.Builder aBuilder = new org.pageseeder.flint.lucene.search.AutoSuggest.Builder();
    aBuilder.index(this._index)
            .directory(autosuggestDir)
            .minChars(min)
            .useTerms(terms)
            .searchFields(fields)
            .weights(weights)
            .resultFields(resultFields);
    // build it
    org.pageseeder.flint.lucene.search.AutoSuggest as = aBuilder.build();
    IndexReader reader = null;
    try {
      reader = grabReader();
      as.build(reader);
    } catch (IndexException ex) {
      LOGGER.error("Failed to build autosuggest", ex);
    } finally {
      if (reader != null) releaseSilently(reader);
    }
    // store it in cache
    this._autosuggests.put(name, as);
    return as;
  }

  private void clearAutoSuggest(String name, org.pageseeder.flint.lucene.search.AutoSuggest as) {
    // close it
    as.close();
    // build name
    String folderName = this._name+"_"+name+"_autosuggest";
    // get folder
    File folder = new File(FlintConfig.get().getRootDirectory(), folderName);
    // delete all files
    for (File f : folder.listFiles()) {
      f.delete();
    }
    // delete folder
    folder.delete();
  }

}