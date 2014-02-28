package com.bluelake.datamodule;

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

import com.bluelake.datamodule.processor.IngestProcessor;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;

public class BqIngestServlet extends HttpServlet {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(BqIngestServlet.class.getName());
  Bigquery bigquery = GcpUtil.createBQClient();

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String projectId = req.getParameter(DataModuleConstants.INGEST_REQUEST_PARAM_PROJECTID);
    String jobId = req.getParameter(DataModuleConstants.INGEST_REQUEST_PARAM_JOBID);
    String entityKey = req.getParameter(DataModuleConstants.INPUT_REQUEST_PARAM_ENTITYKEY);
    String startIndexS = req.getParameter(DataModuleConstants.INGEST_REQUEST_PARAM_STARTINDEX);
    String pageSizeS = req.getParameter(DataModuleConstants.INGEST_REQUEST_PARAM_PAGESIZE);

    if (startIndexS != null) {
      long pageSize;
      if (pageSizeS != null) {
        pageSize = Long.parseLong(pageSizeS);
      } else {
        pageSize = DataModuleConstants.GCS_SPLITSIZE;
      }

      LOG.log(Level.INFO, "startIndex: " + startIndexS);
      LOG.log(Level.INFO, "pageSize: " + pageSize);

      ingestQueryResultToDS(projectId, jobId, "", startIndexS, pageSize);

      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  private void ingestQueryResultToDS(String projectId, String jobId, String entityKey,
      String startIndexS, long pageSize) throws IOException {
    LOG.log(Level.INFO, "start processing");
    LOG.log(Level.INFO, "bq job: " + projectId + "/" + jobId);
    LOG.log(Level.INFO, "startIndex: " + startIndexS);

    String[] entityKeyArray = entityKey.split(",");

    GetQueryResultsResponse queryResult =
        bigquery.jobs().getQueryResults(projectId, jobId)
            .setStartIndex(new BigInteger(startIndexS))
            .setMaxResults(DataModuleConstants.GCS_SPLITSIZE).execute();

    TableSchema tableSchema = queryResult.getSchema();
    List<TableFieldSchema> fieldSchema = tableSchema.getFields();
    TableFieldSchema[] fieldSchemaArray =
        fieldSchema.toArray(new TableFieldSchema[fieldSchema.size()]);

    // for (int i = 0; i < fieldSchema.size(); i++) {
    // LOG.log(Level.INFO, "fieldSchema[" + i + "]: " + fieldSchemaArray[i].getName());
    // }

    // load the user defined IngestProcessor
    IngestProcessor ingestProcessor;
    try {
      GcsClassLoader loader =
          new GcsClassLoader(this.getClass().getClassLoader(), "mac-test", "test.jar");
      /*
       * LOG.log(Level.INFO, "trying out the urlclassloader"); URLClassLoader loader = new
       * URLClassLoader(new URL[] {new URL("https://storage.cloud.google.com/mac-test/test.jar")});
       */
      Class<? extends IngestProcessor> cz =
          loader.loadClass("com.bluelake.datamodule.test.TestIngestProcessor").asSubclass(
              IngestProcessor.class);
      ingestProcessor = cz.newInstance();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "failed to load class, " + e.getMessage(), e);
      ingestProcessor = new IngestProcessor();
    }

    DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
    List<TableRow> rows = queryResult.getRows();
    if (rows != null) {
      for (TableRow row : rows) {
        // for (TableCell field : row.getF()) {
        List<TableCell> fields = row.getF();
        JSONObject jsonObj = new JSONObject();

        try {
          for (int i = 0; i < fields.size(); i++) {
            jsonObj.put(fieldSchemaArray[i].getName(),
                (String) ((TableCell) (fields.get(i))).getV());
            LOG.log(Level.INFO, fieldSchemaArray[i].getName() + ":"
                + (String) ((TableCell) (fields.get(i))).getV());
          }

          Entity entity = ingestProcessor.processRecord(entityKeyArray, jsonObj);
          // datastoreService.put(entity);
        } catch (JSONException je) {
          LOG.log(Level.SEVERE, je.getMessage(), je);
        }
      }
    }
  }
}
