/*
 * This file is part of the Flint library.
 *
 * For licensing information please see the file license.txt included in the release.
 * A copy of this licence can also be found at 
 *   http://www.opensource.org/licenses/artistic-license-2.0.php
 */
package org.weborganic.flint.query;

import java.io.IOException;
import java.util.Date;

import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.weborganic.flint.util.Dates;

import com.topologi.diffx.xml.XMLWriter;

/**
 * Create a date range parameter.
 *
 * @author Christophe Lauret (Weborganic)
 * @author Jean-Baptiste Reure (Weborganic)
 *
 * @version 10 September 2010
 */
public final class DateParameter implements SearchParameter {

  /**
   * The date field.
   */
  private final String _field;

  /**
   * The FROM date, if <code>null</code>, then no lower limit.
   */
  private Date _from;

  /**
   * The TO date, if <code>null</code>, then no upper limit.
   */
  private Date _to;

  /**
   * The resolution for this date field.
   */
  private Resolution _resolution = Resolution.DAY;

  /**
   * Indicates whether the date field is a numeric field.
   */
  private boolean _numeric = false;

  /**
   * The actual Lucene query (lazy initialised)
   */
  private volatile Query _query;

// Implementation methods ----------------------------------------------------------------------

  /**
   * Creates a new date parameter.
   * 
   * @param field      the date field to search
   * @param from       the start date in the range (may be <code>null</code>)
   * @param to         the end date in the range (may be <code>null</code>)
   * @param resolution the date resolution
   * @param numeric    whether it is a numeric field
   */
  public DateParameter(String field, Date from, Date to, Resolution resolution, boolean numeric) {
    if (field == null) throw new NullPointerException("field");
    if (resolution == null) throw new NullPointerException("resolution");
    this._field = field;
    this._from = from;
    this._to = to;
    this._resolution = resolution;
    this._numeric = numeric;
  }

  /**
   * Returns the value of the lower limit of the date range.
   * 
   * @return A date instance or <code>null</code>.
   */
  public Date from() {
    return this._from != null? new Date(this._from.getTime()) : null;
  }

  /**
   * Returns the value of the upper limit for the date range.
   * 
   * @return A date instance  or <code>null</code>.
   */
  public Date to() {
    return this._to != null? new Date(this._to.getTime()) : null;
  }

  /**
   * Returns the name of the date field to search.
   * 
   * @return The name of the date field to search.
   */
  public String field() {
    return this._field;
  }

  /**
   * Returns <code>true</code> if this search query does not contain any parameter.
   * 
   * @return <code>true</code> if this search query does not contain any parameter;
   *         <code>false</code> otherwise.
   */
  public boolean isEmpty() {
    return this._from == null && this._to == null;
  }

  /**
   * Generates the <code>Query</code> object corresponding to a date range search query.
   * 
   * Returns a <code>TermRangeQuery</code> or a <code>NumericRangeQuery</code> based on the values in this object.
   * 
   * @return a <code>TermRangeQuery</code>, a <code>NumericRangeQuery</code> or <code>null</code> if empty.
   */
  public Query toQuery() {
    if (this._from == null && this._to == null) return null;
    // an including range query on the date
    if (this._query == null)  {
      if (this._numeric) {
        this._query = toNumericRangeQuery(this._field, this._from, this._to, this._resolution);
      } else {
        this._query = toTermRangeQuery(this._field, this._from, this._to, this._resolution);
      }
    }
    return this._query;
  }

  /**
   * Serialises the search query as XML.
   * 
   * @param xml The XML writer.
   * 
   * @throws IOException Should there be any I/O exception while writing the XML.
   */
  public void toXML(XMLWriter xml) throws IOException {
    xml.openElement("date-range", false);
    if (this._from != null)
      xml.attribute("from", Dates.format(this._from, this._resolution));
    if (this._to != null)
      xml.attribute("to",  Dates.format(this._to, this._resolution));
    xml.closeElement();
  }

  // Private helpers ------------------------------------------------------------------------------

  /**
   * Returns the term range query that corresponds to the specified parameters.
   * 
   * @param field      the date field
   * @param from       the lower limit (may be null)
   * @param to         the upper limit (may be null)
   * @param resolution the date resolution in use
   * 
   * @return the corresponding <code>TermRangeQuery</code>
   */
  private static TermRangeQuery toTermRangeQuery(String field, Date from, Date to, Resolution resolution) {
    String min = from != null? Dates.toString(from, resolution) : null;
    String max = to != null? Dates.toString(to, resolution) : null;
    return new TermRangeQuery(field, min, max, true, true);
  }

  /**
   * Returns the term range query that corresponds to the specified parameters.
   * 
   * @param field      the date field
   * @param from       the lower limit (may be null)
   * @param to         the upper limit (may be null)
   * @param resolution the date resolution in use
   * 
   * @return the corresponding <code>NumericRangeQuery</code>
   */
  private static NumericRangeQuery<? extends Number> toNumericRangeQuery(String field, Date from, Date to, Resolution resolution) {
    Number min = from != null? Dates.toNumber(from, resolution) : null;
    Number max = to != null? Dates.toNumber(to, resolution) : null;
    // Using long values (resolution = MILLISECOND | SECOND | MINUTE | HOUR)
    if (min instanceof Long) {
      return NumericRangeQuery.newLongRange(field, (Long)min, (Long)max, true, true);
    }
    // Using integer values (resolution = DAY | MONTH | YEAR)
    if (max instanceof Integer) {
      return NumericRangeQuery.newIntRange(field, (Integer)min, (Integer)max, true, true);
    }
    // Should never happen
    return null;
  }

}
