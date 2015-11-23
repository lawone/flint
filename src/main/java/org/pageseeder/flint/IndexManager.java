/*
 * Copyright 2015 Allette Systems (Australia)
 * http://www.allette.com.au
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pageseeder.flint;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.pageseeder.flint.IndexJob.Priority;
import org.pageseeder.flint.api.Content;
import org.pageseeder.flint.api.ContentFetcher;
import org.pageseeder.flint.api.ContentTranslator;
import org.pageseeder.flint.api.ContentTranslatorFactory;
import org.pageseeder.flint.api.ContentType;
import org.pageseeder.flint.api.Index;
import org.pageseeder.flint.api.IndexListener;
import org.pageseeder.flint.api.Requester;
import org.pageseeder.flint.log.NoOpListener;
import org.pageseeder.flint.query.SearchPaging;
import org.pageseeder.flint.query.SearchQuery;
import org.pageseeder.flint.query.SearchResults;
import org.pageseeder.flint.search.Facet;
import org.pageseeder.flint.search.FieldFacet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class from Flint, applications should create one instance of this class.
 *
 * <ul>
 *   <li>To start and stop the indexing thread, use the methods {@link #start()} and {@link #stop()}.</li>
 *   <li>To register IndexConfigs, use the methods registerIndexConfig() and getConfig().</li>
 *   <li>To add/modify/delete content from an Index, use the method {@link #index(ContentId, Index, IndexConfig, Requester, Priority, Map)}</li>
 *   <li>To search an Index, use the methods {@link IndexManager#query()}</li>
 *   <li>to load an Index's statuses, use the method {@link #getStatus()}</li>
 * </ul>
 *
 * @author Jean-Baptiste Reure
 * @authro Christophe Lauret
 *
 * @version 27 February 2013
 */
public final class IndexManager {

  /**
   * Logger will receive debugging and low-level data, use the listener to capture specific indexing operations.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexManager.class);

  /**
   * NB of indexing threads
   */
  private static final int NB_INDEXING_THREADS = 10;

  /**
   * Listens to any problem reported by the indexer.
   */
  private final IndexListener _listener;

  /**
   * The fetcher used to retrieve content to index.
   */
  private final ContentFetcher _fetcher;

  /**
   * The queue were index job are waiting to be run.
   */
  private final IndexJobQueue _indexQueue;

  /**
   * Maps index ID to the indexes IO.
   */
  private final ConcurrentHashMap<String, IndexIO> _indexes;

  /**
   * Maps MIME types to the ContentTranslator factory to use.
   */
  private final ConcurrentHashMap<String, ContentTranslatorFactory> translatorFactories;

  /**
   * Time since the last activity on this manager.
   */
  private final AtomicLong _lastActivity = new AtomicLong(0);

  /**
   * Priority of the thread, default to NORM_PRIORITY (5)
   */
  private int threadPriority = Thread.NORM_PRIORITY;

  /**
   * The thread manager.
   */
  private ExecutorService threadPool = null;

  /**
   * A default Translator, used when no Factory matches a certain MIME Type.
   */
  private ContentTranslator _defaultTranslator = null;

  /**
   * Flag to save the state.
   */
  private boolean checkingForCommit = false;

  /**
   * Simple constructor which will use a SilentListener.
   *
   * @param cf the Content Fetcher used to retrieve the content to index.
   */
  public IndexManager(ContentFetcher cf) {
    this(cf, NoOpListener.getInstance());
  }

  /**
   * Simple Constructor.
   *
   * @param cf       the Content Fetcher used to retrieve the content to index.
   * @param listener an object used to record events
   */
  public IndexManager(ContentFetcher cf, IndexListener listener) {
    this(cf, listener, NB_INDEXING_THREADS);
  }

  /**
   * Simple Constructor.
   *
   * @param cf        the Content Fetcher used to retrieve the content to index.
   * @param listener  an object used to record events
   * @param nbThreads the number of indexing threads
   */
  public IndexManager(ContentFetcher cf, IndexListener listener, int nbThreads) {
    this._fetcher = cf;
    this._listener = listener;
    this._indexQueue = new IndexJobQueue();
    // To initialise the map, use ideally (# of index, high load factor, # of threads writing)
    this._indexes = new ConcurrentHashMap<String, IndexIO>(50, 0.75f, nbThreads);
    // Register default XML factory
    this.translatorFactories = new ConcurrentHashMap<String, ContentTranslatorFactory>(16, 0.8f, 2);
    registerTranslatorFactory(new FlintTranslatorFactory());
    this._lastActivity.set(System.currentTimeMillis());
    // create the worker thread pool
    this.threadPool = Executors.newFixedThreadPool(nbThreads, new ThreadFactory() {
      private int threadCount = 1;
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "indexing-p"+IndexManager.this.threadPriority+"-t"+this.threadCount++);
        t.setPriority(IndexManager.this.threadPriority);
        return t;
      }
    });
  }

  // Public external methods
  // ----------------------------------------------------------------------------------------------

  /**
   * Set the default translator to use when no factory matches a MIME type.
   *
   * <p>This to the {@link ContentTranslator} used when the MIME. Default is <code>null</code>.
   *
   * @param translator the translator to use by default.
   */
  public void setDefaultTranslator(ContentTranslator translator) {
    this._defaultTranslator = translator;
  }

  /**
   * Set the priority of the thread.
   *
   * <p>This has no effect if called after the method <code>start()</code> is called.
   *
   * @param priority the priority of the Indexing Thread (from 1 to 10, default is 5)
   *
   * @throws IndexOutOfBoundsException if the priority is less than 1 or greater than 10.
   */
  public void setThreadPriority(int priority) {
    if (priority < 1 || priority > 10)
      throw new IndexOutOfBoundsException("Thread priority is should be between 1 and 10 but was "+priority);
    this.threadPriority = priority;
  }

  /**
   * Register a new factory with the all the MIME types supported by the factory.
   *
   * <p>If there was already a factory registered for a MIME type, it is overwritten.
   *
   * @param factory The factory to register
   */
  public void registerTranslatorFactory(ContentTranslatorFactory factory) {
    Collection<String> types = factory.getMimeTypesSupported();
    for (String type : types) {
      registerTranslatorFactory(type, factory);
    }
  }

  /**
   * Register a new factory with the given MIME type.
   *
   * <p>If there was already a factory registered for this MIME type, it is overwritten.
   * <p>The factory object must support the MIME type provided
   *
   * @param mimeType the MIME type
   * @param factory  the factory to register
   */
  public void registerTranslatorFactory(String mimeType, ContentTranslatorFactory factory) {
    this.translatorFactories.put(mimeType, factory);
  }

  /**
   * Add a new batch update job to the indexing queue.
   *
   * @param contents the batch contents
   * @param r        the Requester calling this method (used for logging)
   * @param p        the Priority of this job
   * @param params   the dynamic XSLT parameters
   */
  public void indexBatch(Map<String, ContentType> contents, Index i, Requester r, Priority p) {
    IndexJob.Batch batch = new IndexJob.Batch(contents.size());
    for (String key : contents.keySet()) {
      indexJob(IndexJob.newBatchJob(batch, key, contents.get(key), i, p, r));
    }
  }

  /**
   * Add a new update job to the indexing queue.
   *
   * @param content  the ID of the Content
   * @param i        the Index to add the Content to
   * @param r        the Requester calling this method (used for logging)
   * @param p        the Priority of this job
   * @param params   the dynamic XSLT parameters
   */
  public void index(String contentid, ContentType type, Index i, Requester r, Priority p) {
    indexJob(IndexJob.newJob(contentid, type, i, p, r));
  }

  /**
   * Add a new update job to the indexing queue.
   *
   * @param index      the Index to add the Content to
   * @param requester  the Requester calling this method (used for logging)
   * @param priority   the Priority of this job
   */
  public void clear(Index index, Requester requester, Priority priority) {
    indexJob(IndexJob.newClearJob(index, priority, requester));
  }

  /**
   * Returns the list of waiting jobs for the Requester provided.
   *
   * <p>Note that by the time each job is checked, they might have run already so the method
   * {@link IndexJob#isFinished()} should be called before parsing the job.
   *
   * <p>The list will never be <code>null</code>.
   *
   * @param r the Requester
   * @return the list of jobs waiting (never <code>null</code>)
   */
  public List<IndexJob> getStatus(Requester r) {
    return this._indexQueue.getJobsForRequester(r);
  }

  /**
   * Returns the list of waiting jobs for the index provided.
   *
   * <p>Note that by the time each job is checked, they might have run already so the method
   * {@link IndexJob#isFinished()} should be called before parsing the job.
   *
   * <p>The list will never be <code>null</code>.
   *
   * @param i the index
   * @return the list of jobs waiting (never <code>null</code>)
   */
  public List<IndexJob> getStatus(Index i) {
    return this._indexQueue.getJobsForIndex(i);
  }

  /**
   * Returns the list of waiting job for the all the indexes.
   *
   * <p>Note that by the time each job is checked, they might have run already so the method
   * {@link IndexJob#isFinished()} should be called before parsing the job.
   *
   * <p>The list will never be <code>null</code>.
   *
   * @return the list of jobs waiting (never <code>null</code>)
   */
  public List<IndexJob> getStatus() {
    return this._indexQueue.getAllJobs();
  }

  /**
   * Run a search on the given Index.
   *
   * @param index the Index to run the search on
   * @param query the query to run
   * @return the search results
   * @throws IndexException if any error occurred while performing the search
   */
  public SearchResults query(Index index, SearchQuery query) throws IndexException {
    return query(index, query, new SearchPaging());
  }

  /**
   * Run a search on the given Index.
   *
   * @param index  the Index to run the search on
   * @param query  the query to run
   * @param paging paging details (can be <code>null</code>)
   *
   * @return the search results
   *
   * @throws IndexException if any error occurred while performing the search
   */
  public SearchResults query(Index index, SearchQuery query, SearchPaging paging) throws IndexException {
    IndexIO io = getIndexIO(index);
    IndexSearcher searcher = io.bookSearcher();
    if (searcher != null) {
      try {
        Query lquery = query.toQuery();
        if (lquery == null) {
          io.releaseSearcher(searcher);
          throw new IndexException("Failed performing a query on the Index because the query is null", new NullPointerException("Null query"));
        }
        LOGGER.debug("Performing search [{}] on index {}", query, index);
        Sort sort = query.getSort();
        if (sort == null) {
          sort = Sort.INDEXORDER;
        }
        // load the scores
        TopFieldCollector tfc = TopFieldCollector.create(sort, paging.getHitsPerPage() * paging.getPage(), true, true, false);
        searcher.search(lquery, tfc);
        return new SearchResults(query, tfc.topDocs().scoreDocs, tfc.getTotalHits(), paging, io, searcher);
      } catch (IOException e) {
        io.releaseSearcher(searcher);
        throw new IndexException("Failed performing a query on the Index because of an I/O problem", e);
      }
    }
    return null;
  }

  /**
   * Run a search on the given Index.
   *
   * @param index  the Index to run the search on
   * @param query  the query to run
   * @param paging paging details (can be <code>null</code>)
   *
   * @return the search results
   *
   * @throws IndexException if any error occurred while performing the search
   */
  public void query(Index index, Query query, Collector results) throws IndexException {
    IndexIO io = getIndexIO(index);
    IndexSearcher searcher = io.bookSearcher();
    if (searcher != null) {
      try {
        LOGGER.debug("Performing search [{}] on index {}", query, index);
        // load the scores
        searcher.search(query, results);
      } catch (IOException e) {
        throw new IndexException("Failed performing a query on the Index because of an I/O problem", e);
      } finally {
        io.releaseSearcher(searcher);
      }
    }
  }

  /**
   * Run a search on the given Indexes.
   *
   * @param indexes the Indexes to run the search on
   * @param query   the query to run
   * 
   * @return the search results
   * 
   * @throws IndexException if any error occurred while performing the search
   */
  public SearchResults query(List<Index> indexes, SearchQuery query) throws IndexException {
    return query(indexes, query, new SearchPaging());
  }

  /**
   * Run a search on the given Indexes.
   *
   * @param indexes  the Indexes to run the search on
   * @param query    the query to run
   * @param paging   paging details (can be <code>null</code>)
   *
   * @return the search results
   *
   * @throws IndexException if any error occurred while performing the search
   */
  public SearchResults query(List<Index> indexes, SearchQuery query, SearchPaging paging) throws IndexException {
    Query lquery = query.toQuery();
    if (lquery == null)
      throw new IndexException("Failed performing a query because the query is null", new NullPointerException("Null query"));
    Map<IndexIO, IndexReader> readersMap = new HashMap<>();
    IndexReader[] readers = new IndexReader[indexes.size()];
    // grab a reader for each indexes
    for (int i = 0; i < indexes.size(); i++) {
      IndexIO io = getIndexIO(indexes.get(i));
      // make sure index has been setup
      if (io != null) {
        // grab what we need
        IndexReader reader = io.bookReader();
        readers[i] = reader;
        readersMap.put(io, reader);
      }
    }
    try {
      MultiReader reader = new MultiReader(readers);
      IndexSearcher searcher = new IndexSearcher(reader);
      LOGGER.debug("Performing search [{}] on {} indexes", query, readers.length);
      Sort sort = query.getSort();
      if (sort == null) sort = Sort.INDEXORDER;
      // load the scores
      TopFieldDocs results = searcher.search(lquery, paging.getHitsPerPage() * paging.getPage(), sort);
      return new SearchResults(query, results, paging, readersMap, searcher);
    } catch (IOException e) {
      for (IndexIO io : readersMap.keySet())
        io.releaseReader(readersMap.get(io));
      throw new IndexException("Failed performing a query on the Index because of an I/O problem", e);
    }
  }

  /**
   * Returns the list of term and how frequently they are used by performing a fuzzy match on the
   * specified term.
   *
   * @param field  the field to use as a facet
   * @param upTo   the max number of values to return
   * @param query  a predicate to apply on the facet (can be null or empty)
   *
   * @return the facte instance.
   *
   * @throws IOException    if there was an error reading the index or creating the condition query
   * @throws IndexException if there was an error getting the reader or searcher.
   */
  public Facet getFacet(String field, int upTo, Query query, Index index) throws IndexException, IOException {
    FieldFacet facet = null;
    IndexReader reader = null;
    IndexSearcher searcher = null;
    try {
      // Retrieve all terms for the field
      reader = grabReader(index);
      facet = FieldFacet.newFacet(field, reader);

      // search
      searcher = grabSearcher(index);
      facet.compute(searcher, query, upTo);

    } finally {
      releaseQuietly(index, reader);
      releaseQuietly(index, searcher);
    }
    return facet;
  }

  /**
   * Returns the list of term and how frequently they are used by performing a fuzzy match on the
   * specified term.
   *
   * @param fields the fields to use as facets
   * @param upTo   the max number of values to return
   * @param query  a predicate to apply on the facet (can be null or empty)
   *
   * @throws IndexException if there was an error reading the indexes or creating the condition query
   * @throws IllegalStateException If one of the indexes is not initialised
   */
  public List<Facet> getFacets(List<String> fields, int upTo, Query query, Index index) throws IOException, IndexException {
    // parameter checks
    if (fields == null || fields.isEmpty() || index == null)
      return Collections.emptyList();
    List<Facet> facets = new ArrayList<Facet>();
    for (String field : fields) {
      if (field.length() > 0) {
        facets.add(getFacet(field, upTo, query, index));
      }
    }
    return facets;
  }

  /**
   * Returns the list of term and how frequently they are used by performing a fuzzy match on the
   * specified term.
   *
   * @param fields the fields to use as facets
   * @param upTo   the max number of values to return
   * @param query  a predicate to apply on the facet (can be null or empty)
   *
   * @throws IndexException if there was an error reading the indexes or creating the condition query
   * @throws IllegalStateException If one of the indexes is not initialised
   */
  public List<Facet> getFacets(List<String> fields, int upTo, Query query, List<Index> indexes) throws IOException, IndexException {
    // parameter checks
    if (fields == null || fields.isEmpty() || indexes.isEmpty())
      return Collections.emptyList();
    // check for one index only
    if (indexes.size() == 1)
      return getFacets(fields, upTo, query, indexes.get(0));
    // retrieve all searchers and readers
    IndexReader[] readers = new IndexReader[indexes.size()];
    IndexIO[] ios = new IndexIO[indexes.size()];
    // grab a reader for each indexes
    for (int i = 0; i < indexes.size(); i++) {
      Index index = indexes.get(i);
      ios[i] = getIndexIO(index);
      readers[i] = grabReader(index);
    }
    List<Facet> facets = new ArrayList<Facet>();
    try {
      // Retrieve all terms for the field
      IndexReader multiReader = new MultiReader(readers);
      IndexSearcher multiSearcher = new IndexSearcher(multiReader);
      for (String field : fields) {
        if (field.length() > 0) {
          FieldFacet facet = FieldFacet.newFacet(field, multiReader);
          // search
          facet.compute(multiSearcher, query, upTo);
          // store it
          facets.add(facet);
        }
      }
    } finally {
      // now release everything we used
      for (int i = 0; i < ios.length; i++)  {
        ios[i].releaseReader(readers[i]);
      }
    }
    return facets;
  }

  public MultipleIndexReader getMultipleIndexReader(List<Index> indexes) {
    return new MultipleIndexReader(this, indexes);
  }

  // Lower level API providing access to Lucene objects
  // ----------------------------------------------------------------------------------------------

  /**
   * Returns a near real-time Reader on the index provided.
   *
   * <p>IMPORTANT: the reader should not be closed, it should be used in the following way to ensure
   *  it is made available to other threads:</p>
   * <pre>
   *    IndexReader reader = manager.grabReader(index);
   *    try {
   *      ...
   *    } finally {
   *      manager.release(index, reader);
   *    }
   * </pre>
   *
   * @param index the index that the Index Reader will point to.
   * @return the Index Reader to read from the index
   *
   * @throws IndexException If an IO error occurred when getting the reader.
   */
  public IndexReader grabReader(Index index) throws IndexException {
    return getIndexIO(index).bookReader();
  }

  /**
   * Release an {@link IndexReader} after it has been used.
   *
   * <p>It is necessary to release a reader so that it can be reused for other threads.
   *
   * @see IndexManager#grabReader(Index)
   *
   * @param index  The index the reader works on.
   * @param reader The actual Lucene index reader.
   *
   * @throws IndexException Wrapping any IO exception
   */
  public void release(Index index, IndexReader reader) throws IndexException {
    if (reader == null) return;
    getIndexIO(index).releaseReader(reader);
  }

  /**
   * Releases an {@link IndexReader} quietly after it has been used so that it can be used in a <code>finally</code>
   * block.
   *
   * <p>It is necessary to release a reader so that it can be reused for other threads.
   *
   * @see IndexManager#grabReader(Index)
   *
   * @param index  The index the reader works on.
   * @param reader The actual Lucene index reader.
   */
  public void releaseQuietly(Index index, IndexReader reader) {
    if (reader == null) return;
    try {
      getIndexIO(index).releaseReader(reader);
    } catch (IndexException ex) {
      LOGGER.error("Failed to release a reader because of an Index problem", ex);
    }
  }

  /**
   * Returns a near real-time Searcher on the index provided.
   *
   * <p>IMPORTANT: the searcher should not be closed, it should be used in the following way to
   * ensure it is made available to other threads:</p>
   * <pre>
   *    IndexSearcher searcher = manager.grabSearcher(index);
   *    try {
   *      ...
   *    } finally {
   *      manager.release(index, searcher);
   *    }
   * </pre>
   *
   * @param index the index that the searcher will work on.
   * @return the index searcher to use on the index
   *
   * @throws IndexException If an IO error occurred when getting the reader.
   */
  public IndexSearcher grabSearcher(Index index) throws IndexException {
    IndexIO io = getIndexIO(index);
    return io.bookSearcher();
  }

  /**
   * Release an {@link IndexSearcher} after it has been used.
   *
   * <p>It is necessary to release a searcher so that it can be reused by other threads.
   *
   * @see IndexManager#grabSearcher(Index)
   *
   * @param index    The index the searcher works on.
   * @param searcher The actual Lucene index searcher.
   *
   * @throws IndexException Wrapping any IO exception
   */
  public void release(Index index, IndexSearcher searcher) throws IndexException {
    if (searcher == null) return;
    IndexIO io = getIndexIO(index);
    io.releaseSearcher(searcher);
  }

  /**
   * Releases an {@link IndexSearcher} quietly after it has been used so that it can be used in a <code>finally</code>
   * block.
   *
   * <p>It is necessary to release a searcher so that it can be reused for other threads.
   *
   * @see IndexManager#grabReader(Index)
   *
   * @param index    The index the searcher works on.
   * @param searcher The actual Lucene index searcher.
   */
  public void releaseQuietly(Index index, IndexSearcher searcher) {
    if (searcher == null) return;
    try {
      getIndexIO(index).releaseSearcher(searcher);
    } catch (IndexException ex) {
      LOGGER.error("Failed to release a searcher - quietly ignoring", ex);
    }
  }

  /**
   * Translate content into IDX data.
   *
   * @param type    the Content Type
   * @param config  the index config, where the XSLT script is registered
   * @param content the actual Content to transform
   * @param params  the parameters to add to the translation
   * @param out     the Writer to write the result to
   * @throws IndexException if anything went wrong
   */
  public void translateContent(Index index, Content content, Map<String, String> params, Writer out) throws IndexException {
    IndexingThread.translateContent(this, null, index, content, params, out);
  }

  public long getLastTimeUsed(Index index) {
    if (index == null) return -1;
    // get index IO if used
    if (this._indexes.contains(index.getIndexID()))
      return this._indexes.get(index.getIndexID()).getLastTimeUsed();
    // get last commit data then
    try {
      List<IndexCommit> commits = DirectoryReader.listCommits(index.getIndexDirectory());
      if (commits == null || commits.isEmpty()) return -1;
      String lastCommitDate = commits.get(commits.size()-1).getUserData().get(IndexIO.LAST_COMMIT_DATE);
      if (lastCommitDate != null) return Long.parseLong(lastCommitDate);
    } catch (IOException ex) {
      LOGGER.error("Failed to load last index commit date for "+index.getIndexID(), ex);
    }
    return -1;
  }

  /**
   * Kills the thread and close all the indexes.
   */
  public void stop() {
    // empty queue
    this._indexQueue.clear();
    // Stop the threads
    this.threadPool.shutdown();
    // Close all indexes
    for (Entry<String, IndexIO> e : this._indexes.entrySet()) {
      String id = e.getKey();
      IndexIO index = e.getValue();
      try {
        index.stop();
      } catch (IndexException ex) {
        LOGGER.error("Failed to close Index {}: {}", id, ex.getMessage(), ex);
      }
    }
  }

  // protected methods used by the threads

  /**
   * Get a translator to turn content into Flint Index XML
   *
   * @param mediatype the content media type
   *
   * @throws IndexException if anything went wrong
   */
  protected ContentTranslator getTranslator(String mediatype) throws IndexException {
    if (mediatype == null) throw new NullPointerException("mediatype");
    ContentTranslatorFactory factory = this.translatorFactories.get(mediatype);
    // no factory found
    if (factory == null && this._defaultTranslator == null)
      throw new IndexException("Media Type "+mediatype+" is not supported, no Translator Factory was found and no default Translator was specified.", null);
    // load translator
    ContentTranslator translator = factory == null ? this._defaultTranslator : factory.createTranslator(mediatype);
    if (translator == null)
      throw new IndexException("No translator was found for MIME Type "+mediatype+".", null);
    return translator;
  }

  /**
   * 
   * @param contentid
   * @param contenttype
   * @return
   * @throws IndexException
   */
  protected Content getContent(String contentid, ContentType contenttype) throws IndexException {
    // retrieve content
    return this._fetcher.getContent(contentid, contenttype);
  }

  // Private helpers ==============================================================================

  /**
   * Start an index job.
   */
  private void indexJob(IndexJob job) {
    // add job to queue
    this._indexQueue.addJob(job);
    // start thread to index it
    this.threadPool.execute(new IndexingThread(this, this._listener, this._indexQueue));
  }

  /**
   * Retrieves an IndexIO, creates it if non existent.
   *
   * @param index the index requiring the IO utility.
   * @return
   * @throws IndexException
   */
  public synchronized IndexIO getIndexIO(Index index) throws IndexException {
    if (index == null) return null;
    IndexIO io = this._indexes.get(index.getIndexID());
    if (io == null) {
      LOGGER.debug("Creating a new IndexIO for {}", index.getIndexID());
      io = new IndexIO(index);
      this._indexes.put(index.getIndexID(), io);
    }
    return io;
  }

  /**
   * Loop through the index and check if any of them need committing, also checks if they can be optimized.
   */
  public void checkForCommit() {
    // don't do it if we're still indexing or if we're already doing it
    if (this.checkingForCommit ||!this._indexQueue.isEmpty()) return;
    LOGGER.debug("Checking for commits");
    this.checkingForCommit = true;
    // loop through the indexes and check which one needs committing
    List<IndexIO> ios = new ArrayList<IndexIO>(this._indexes.values());
    List<IndexIO> closed = new ArrayList<IndexIO>();
    for (IndexIO io : ios) {
      if (io.isClosed()) {
        closed.add(io);
      } else {
        try {
          io.maybeCommit();
        } catch (IndexException ex) {
          LOGGER.error("Failed to perform commit", ex);
        }
      }
      // make sure there's no job waiting
      if (!this._indexQueue.isEmpty()) break;
    }
    // removed closed IOs
    for (IndexIO io : closed) {
      this._indexes.remove(io);
    }
    this.checkingForCommit = false;
  }

}
