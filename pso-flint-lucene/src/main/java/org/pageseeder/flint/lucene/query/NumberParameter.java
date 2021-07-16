package org.pageseeder.flint.lucene.query;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.pageseeder.flint.catalog.Catalog;
import org.pageseeder.flint.catalog.Catalogs;
import org.pageseeder.flint.indexing.FlintField.NumericType;
import org.pageseeder.flint.lucene.util.Beta;
import org.pageseeder.xmlwriter.XMLWriter;

import java.io.IOException;

/**
 * Create a number parameter using a numeric value.
 *
 * This is API is still experimental and subject to change in Lucene, any change in Lucene may also
 * be reflected in this API.
 *
 * @param <T> The number type for this number search
 *
 * @author Christophe Lauret (Weborganic)
 * @author Jean-Baptiste Reure (Weborganic)
 *
 * @version 29 June 2016
 */
@Beta
public class NumberParameter<T extends Number> implements SearchParameter {

  /**
   * The numeric field.
   */
  private final String _field;

  /**
   * The value to search for.
   */
  private final T _value;

  /**
   * The actual Lucene query (lazy initialised)
   */
  private volatile Query _query;

  /**
   * Creates a new number parameter.
   *
   * @param field  the numeric field to search
   * @param value  the value to search for
   */
  public NumberParameter(String field, T value) {
    if (field == null) throw new NullPointerException("field");
    this._field = field;
    this._value = value;
  }

  @Override
  public void toXML(XMLWriter xml) throws IOException {
    xml.openElement("number-parameter", false);
    xml.attribute("field", this._field);
    if (this._value != null) {
      xml.attribute("type",  this._value.getClass().getName().replaceFirst("^(.+\\.)", "").toLowerCase());
      xml.attribute("value", String.valueOf(this._value));
    }
    xml.closeElement();
  }

  @Override
  public boolean isEmpty() {
    return this._value == null;
  }

  @Override
  public Query toQuery() {
    if (this._value == null) return null;
    if (this._query == null)  {
      // use class of value field
      if (this._value instanceof Float)
        this._query = FloatPoint.newExactQuery(this._field, (Float) this._value);
      else if (this._value instanceof Double)
        this._query = DoublePoint.newExactQuery(this._field, (Double) this._value);
      else if (this._value instanceof Integer)
        this._query = IntPoint.newExactQuery(this._field, (Integer) this._value);
      else if (this._value instanceof Long)
        this._query = LongPoint.newExactQuery(this._field, (Long) this._value);
    }
    return this._query;
  }



  // factory methods ------------------------------------------------------------------------------

  /**
   * Factory that creates a <code>NumberParameter</code>, that queries a double value.
   *
   * @param field   the numeric field to search
   * @param value   the value to search
   *
   * @return a new search parameter.
   */
  public static NumberParameter<Double> newDoubleParameter(String field, Double value) {
    return new NumberParameter<>(field, value);
  }

  /**
   * Factory that creates a <code>NumberParameter</code>, that queries a float value.
   *
   * @param field   the numeric field to search
   * @param value   the value to search
   *
   * @return a new search parameter.
   */
  public static NumberParameter<Float> newFloatParameter(String field, Float value) {
    return new NumberParameter<>(field, value);
  }

  /**
   * Factory that creates a <code>NumberParameter</code>, that queries an int value.
   *
   * @param field   the numeric field to search
   * @param value   the value to search
   *
   * @return a new search parameter.
   */
  public static NumberParameter<Integer> newIntParameter(String field, Integer value) {
    return new NumberParameter<>(field, value);
  }

  /**
   * Factory that creates a <code>NumberParameter</code>, that queries a long value.
   *
   * @param field   the numeric field to search
   * @param value   the value to search
   *
   * @return a new search parameter.
   */
  public static NumberParameter<Long> newLongParameter(String field, Long value) {
    return new NumberParameter<>(field, value);
  }

  /**
   * Factory that creates a <code>NumberParameter</code>, used to query a numeric field.
   *
   * @param field    the numeric field to search
   * @param catalog  the name of the catalog, used to ensure the numeric type is correct
   * @param value    the value to search for
   *
   * @return a new number parameter.
   */
  public static NumberParameter<?> newNumberParameter(String field, String catalog, Number value) {
    Catalog thecatalog = Catalogs.getCatalog(catalog);
    if (thecatalog == null) return null;
    NumericType nt = thecatalog.getNumericType(field);
    if (nt == null) return null;
    switch (nt) {
      case DOUBLE: return newDoubleParameter(field, (Double)  value);
      case FLOAT : return newFloatParameter(field,  (Float)   value);
      case INT   : return newIntParameter(field,    (Integer) value);
      case LONG  : return newLongParameter(field,   (Long)    value);
    }
    return null;
  }
}
