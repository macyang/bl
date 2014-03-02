package com.bluelake.datahub;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datahub.udf.DefaultTableSplitUdf;
import com.bluelake.datahub.udf.TestUdf;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class BqIngestServlet extends HttpServlet {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(BqIngestServlet.class.getName());
  Bigquery bigquery = GcpUtil.createBQClient();

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

    try {
      String jobObjS = req.getParameter(DataHubConstants.JOBOBJECT);
      JSONObject jobObj = new JSONObject(jobObjS);
      JSONObject bqObj = jobObj.getJSONObject(Jobs.FIELD_BQ);
      String projectId = bqObj.getString(Jobs.BQ_PROJECTID);
      String jobId = bqObj.getString(Jobs.BQ_JOBID);
      long splitSize = bqObj.getLong(Jobs.BQ_SPLIT);
      String startIndexS = req.getParameter(DataHubConstants.INGEST_REQUEST_PARAM_STARTINDEX);

      if (startIndexS != null) {
        LOG.log(Level.INFO, "startIndex: " + startIndexS);
        LOG.log(Level.INFO, "splitSize: " + splitSize);
        
        GetQueryResultsResponse queryResult =
            bigquery.jobs().getQueryResults(projectId, jobId)
                .setStartIndex(new BigInteger(startIndexS))
                .setMaxResults(splitSize).execute();
        
        DefaultTableSplitUdf tableSplitUdf = new DefaultTableSplitUdf();
        tableSplitUdf.processTableSplit(queryResult);
        
        resp.setStatus(HttpServletResponse.SC_OK);
      }
    } catch (JSONException je) {
      LOG.log(Level.SEVERE, je.getMessage(), je);
    }
  }

  private void ingestQueryResultToDS(String projectId, String jobId, String entityKey,
      String startIndexS, long splitSize) throws IOException {

    String[] entityKeyArray = entityKey.split(",");

    GetQueryResultsResponse queryResult =
        bigquery.jobs().getQueryResults(projectId, jobId)
            .setStartIndex(new BigInteger(startIndexS))
            .setMaxResults(splitSize).execute();

    TableSchema tableSchema = queryResult.getSchema();
    List<TableFieldSchema> fieldSchema = tableSchema.getFields();
    
//    TableFieldSchema[] fieldSchemaArray =
//        fieldSchema.toArray(new TableFieldSchema[fieldSchema.size()]);
//
//    for (int i = 0; i < fieldSchema.size(); i++) {
//      LOG.log(Level.FINER, "fieldSchema[" + i + "]: " + fieldSchemaArray[i].getName());
//    }

    // load the user defined IngestProcessor
//    IngestProcessor ingestProcessor;
//    try {
//      GcsClassLoader loader =
//          new GcsClassLoader(this.getClass().getClassLoader(), "mac-test", "test.jar");
//      /*
//       * LOG.log(Level.INFO, "trying out the urlclassloader"); URLClassLoader loader = new
//       * URLClassLoader(new URL[] {new URL("https://storage.cloud.google.com/mac-test/test.jar")});
//       */
//      Class<? extends IngestProcessor> cz =
//          loader.loadClass("com.bluelake.datahub.test.TestIngestProcessor").asSubclass(
//              IngestProcessor.class);
//      ingestProcessor = cz.newInstance();
//    } catch (Exception e) {
//      LOG.log(Level.SEVERE, "failed to load class, " + e.getMessage(), e);
//      ingestProcessor = new IngestProcessor();
//    }

    List<TableRow> rows = queryResult.getRows();
    if (rows != null) {
      LOG.log(Level.INFO, "start processing table rows");
      for (TableRow row : rows) {
        try {
          JSONObject jsonObj = bqRowToJSON(row, fieldSchema);
          TestUdf.processRecord(entityKeyArray, jsonObj);
        } catch (JSONException je) {
          LOG.log(Level.SEVERE, je.getMessage(), je);
        }
      }
      LOG.log(Level.INFO, "done processing table rows");
    }
  }

  /*
   * Direct mapping from a BigQuery TableRow to a JSONObject. The key name is the column name and
   * the value is the TableCell value casted to String.
   */
  private JSONObject bqRowToJSON(TableRow row, List<TableFieldSchema> fieldSchema)
      throws JSONException {
    List<TableCell> fields = row.getF();
    JSONObject jsonObj = new JSONObject();

    for (int i = 0; i < fields.size(); i++) {
      jsonObj.put(fieldSchema.get(i).getName(), (String) ((TableCell) (fields.get(i))).getV());
      LOG.log(Level.FINER,
          fieldSchema.get(i).getName() + ":" + (String) ((TableCell) (fields.get(i))).getV());
    }
    return jsonObj;
  }
}
