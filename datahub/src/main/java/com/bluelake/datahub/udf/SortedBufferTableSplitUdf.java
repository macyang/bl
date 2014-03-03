package com.bluelake.datahub.udf;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datahub.BqService;
import com.bluelake.datahub.util.SortedBufferEntity;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class SortedBufferTableSplitUdf implements TableSplitUdf {

  private static final Logger LOG = Logger.getLogger(SortedBufferTableSplitUdf.class.getName());

  @Override
  public void init(JSONObject jobObj) {

    // try {
    // JSONObject bqObj = jobObj.getJSONObject(Jobs.FIELD_BQ);
    // } catch (JSONException e) {
    // LOG.log(Level.INFO, e.getMessage(), e);
    // }
  }

  @Override
  public final void processTableSplit(GetQueryResultsResponse queryResult) {

    TableSchema tableSchema = queryResult.getSchema();
    List<TableFieldSchema> fieldSchema = tableSchema.getFields();
    List<TableRow> rows = queryResult.getRows();
    if (rows != null) {
      LOG.log(Level.INFO, "start processing table rows");
      for (TableRow row : rows) {
        try {
          JSONObject rowObj = BqService.bqRowToJSON(row, fieldSchema);
          processTableRow(rowObj);
        } catch (JSONException je) {
          LOG.log(Level.SEVERE, je.getMessage(), je);
        }
      }
      LOG.log(Level.INFO, "done processing table rows");
    }
  }

  /*-
   * This implementation assumes there are three special columns: __kind_, __key_, __id and __data.
   * __kind : The entity kind
   * __key  : The name of the row key
   * __id   : The name of the property
   * __data : The value of the property, a JSONObject string
   * Any remaining columns are added to the _data_ JSONObject using the column name as the key
   */
  protected void processTableRow(JSONObject rowObj) throws JSONException {
    String kind = rowObj.getString("__kind");
    String key = rowObj.getString("__key");
    String id = rowObj.getString("__id");
    String dataS = rowObj.getString("__data");

    SortedBufferEntity sortedBuffer = new SortedBufferEntity(kind, key, 10);

    JSONObject dataObj = new JSONObject(dataS);
    Iterator<?> keys = rowObj.keys();

    while (keys.hasNext()) {
      String k = (String) keys.next();
      if (!k.startsWith("__")) {
        dataObj.put(k, rowObj.get(k));
      }
    }

    sortedBuffer.storeDataAt(id, dataObj);
    sortedBuffer.put();
  }

}
