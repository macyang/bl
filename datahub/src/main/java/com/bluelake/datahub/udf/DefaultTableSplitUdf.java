package com.bluelake.datahub.udf;

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

public class DefaultTableSplitUdf implements TableSplitUdf {
  
  private static final Logger LOG = Logger.getLogger(DefaultTableSplitUdf.class.getName());
  
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
  
  protected void processTableRow(JSONObject rowObj) throws JSONException {
    String imei = rowObj.getString("imei");
    String devicetime = rowObj.getString("devicetime");
    String event = rowObj.getString("event");
    RingBufferEntity ringBuffer = new RingBufferEntity(getEntityKind(), imei, 10);
    try {
      JSONObject obj = new JSONObject(event);
      obj.remove("ID");
      obj.put("devicetime", devicetime);
      ringBuffer.store(obj);
      ringBuffer.put();
    } catch (JSONException je) {
      LOG.log(Level.SEVERE, je.getMessage(), je);
    }
  }
  
  protected final String getEntityKind() {
    return entityKind;
  }

}
