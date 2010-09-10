package org.weborganic.flint.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.weborganic.flint.util.Beta;
import org.weborganic.flint.util.Terms;

import com.topologi.diffx.xml.XMLWriter;

/**
 * A Search query for use as auto suggest. 
 * 
 * @author Christophe Lauret
 * @version 21 July 2010
 */
@Beta
public final class SuggestionQuery implements SearchQuery, FlintQuery {

  /**
   * The list of terms. 
   */
  private final List<Term> _terms;

  /**
   * The condition that all the suggested results must meet.
   */
  private final Query _condition;

  /**
   * The underlying boolean query.
   */
  private BooleanQuery query = null;

  /**
   * Create a new auto-suggest query for the specified list of terms with no condition.
   * 
   * @param terms     The list of terms that should be matched.
   */
  public SuggestionQuery(List<Term> terms) {
    this(terms, null);
  }

  /**
   * Create a new auto-suggest query for the specified list of terms.
   * 
   * @param terms     The list of terms that should be matched.
   * @param condition The condition that must be met by all suggested results (may be <code>null</code>).
   */
  public SuggestionQuery(List<Term> terms, Query condition) {
    this._terms = terms;
    this._condition = condition;
  }

  /**
   * Computes the list of terms to generate the actual suggestion query.
   * 
   * @param reader Computes the list.
   * @throws IOException should an error occurs while reading the index. 
   */
  public void compute(IndexReader reader) throws IOException {
    // Compute the list of terms
    List<Term> terms = new ArrayList<Term>();
    for (Term term : this._terms) {
      Terms.prefix(reader, terms, term);
    }
    // Generate the query
    BooleanQuery bq = new BooleanQuery();
    for (Term t : terms) {
      TermQuery q = new TermQuery(t);
      bq.add(q, Occur.SHOULD);
    }
    // Any condition ?
    if (this._condition != null) {
      this.query = new BooleanQuery();
      this.query.add(this._condition, Occur.MUST);
      this.query.add(bq, Occur.MUST);
    } else {
      this.query = bq;
    }
  }

  /**
   * Prints this query as XML on the specified XML writer.
   * 
   * <p>XML:
   * <pre>{@code
   *  <suggestion-query>
   *    <terms>
   *      <term field="[field]" text="[text]"/>
   *      <term field="[field]" text="[text]"/>
   *      <term field="[field]" text="[text]"/>
   *      ...
   *    </terms>
   *    <condition>
   *     <!-- Condition as a Lucene Query -->
   *     ...
   *    </condition>
   *  </suggestion-query>
   * }</pre>
   * 
   * @see Query#toString()
   * 
   * @param xml The XML writer to use.
   * 
   * @throws IOException If thrown by 
   */
  public void toXML(XMLWriter xml) throws IOException {
    xml.openElement("suggestion-query");
    xml.openElement("terms");
    for (Term t : this._terms) {
      xml.openElement("term");
      xml.attribute("field", t.field());
      xml.attribute("text", t.text());
      xml.closeElement();
    }
    xml.closeElement();
    if (this._condition != null) {
      xml.openElement("condition");
      xml.writeText(this._condition.toString());
      xml.closeElement();
    }
    xml.closeElement();
  }

  /**
   * {@inheritDoc}
   */
  public Query toQuery() {
    return this.query;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isEmpty() {
    return this._terms.isEmpty();
  }

  /**
   * Always sort by relevance.
   * 
   * {@inheritDoc}
   */
  public Sort getSort() {
    return Sort.RELEVANCE;
  }

  /**
   * Always sorts by relevance.
   * 
   * {@inheritDoc}
   */
  public String getPredicate() {
    return this.query != null? this.query.toString() : null;
  }

  /**
   * Returns the field of the first entered term or <code>null</code>.
   * 
   * {@inheritDoc}
   */
  public String getField() {
    if (this._terms.isEmpty()) return null;
    return this._terms.get(0).field();
  }

};

