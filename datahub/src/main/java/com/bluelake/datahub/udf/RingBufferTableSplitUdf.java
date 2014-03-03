package com.bluelake.datahub.udf;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datahub.BqService;
import com.bluelake.datahub.Jobs;
import com.bluelake.datahub.util.RingBufferEntity;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class RingBufferTableSplitUdf implements TableSplitUdf {

  private static final Logger LOG = Logger.getLogger(RingBufferTableSplitUdf.class.getName());

  private String entityKind = "dfaultentitykind";

  @Override
  public void init(JSONObject jobObj) {

    try {
      JSONObject bqObj = jobObj.getJSONObject(Jobs.FIELD_BQ);
      entityKind = bqObj.getString(Jobs.BQ_ENTITYKIND);
    } catch (JSONException e) {
      LOG.log(Level.INFO, e.getMessage(), e);
    }
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
   * This implementation assumes there are three special columns: _kind_, _key_ and _data_
   * __kind : The string value of this column is used as the entity kind
   * __key : The string value of this column is used as the name of the row key
   * __data : This string value of this column should be a JSONObject
   * Any remaining columns are added to the _data_ JSONObject using the column name as the key
   */
  protected void processTableRow(JSONObject rowObj) throws JSONException {
    String kind = rowObj.getString("__kind");
    String key = rowObj.getString("__key");
    String dataS = rowObj.getString("__data");
    
    RingBufferEntity ringBuffer = new RingBufferEntity(kind, key, 10);

    JSONObject dataObj = new JSONObject(dataS);
    Iterator<?> keys = rowObj.keys();

    while (keys.hasNext()) {
      String k = (String) keys.next();
      if (!k.startsWith("__")) {
        dataObj.put(k, rowObj.get(k));
      }
    }

    ringBuffer.store(dataObj);
    ringBuffer.put();
  }

  protected final String getEntityKind() {
    return entityKind;
  }

}
