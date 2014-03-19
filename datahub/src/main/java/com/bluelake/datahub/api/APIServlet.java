package com.bluelake.datahub.api;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datahub.BqService;
import com.bluelake.datahub.GcpUtil;
import com.bluelake.datahub.Jobs;
import com.bluelake.datahub.util.RingBufferEntity;
import com.bluelake.datahub.util.SortedBufferEntity;

public class APIServlet extends HttpServlet {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(APIServlet.class.getName());

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    // /kind/key
    String[] pathElements = req.getPathInfo().split("/");
    if (pathElements.length < 3) {
      throw new IOException("Resource name and id are required.");
    }

    // the first element is the resource name (entity kind)
    // the second element is the id (entity key)
    String kind = pathElements[1];
    String key = pathElements[2];
    
    // TODO : this is a hack to handle something like http://.../bqinfo/table?project=x&dataset=x&table=x
    if ("bqinfo".equals(kind)) {
      if ("table".equals(key)) {
        String projectId = req.getParameter("project");
        String datasetId = req.getParameter("dataset");
        String tableId   = req.getParameter("table");
        
        resp.setContentType("application/json");
        resp.getWriter().print(BqService.bqTableInfo(projectId, datasetId, tableId));
        resp.setStatus(HttpServletResponse.SC_OK);
      }
    }
    else {
    // TODO : dynamically switch between different types of entity, some kind of output formatter?
    // RingBufferEntity entity = new RingBufferEntity(kind, key, 6);
    SortedBufferEntity entity = new SortedBufferEntity(kind, key, 6);
    JSONArray result = new JSONArray();
    for (JSONObject obj : entity) {
      result.put(obj);
    }
    resp.setContentType("application/json");
    resp.getWriter().print(result.toString());
    resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String[] pathElements = req.getPathInfo().split("/");
    if (pathElements.length < 2) {
      throw new IOException("Resource name is required");
    }
    JSONObject jsonReq = GcpUtil.getJSONRequest(req);
    JSONObject jsonResp = null;
    try {
      String resourceKind = jsonReq.getString(Jobs.FIELD_KIND);  
      if (Jobs.RESOURCE_JOB.equalsIgnoreCase(resourceKind)) {
        jsonResp = Jobs.insert(jsonReq);
      }
      resp.setContentType("application/json");
      resp.getWriter().print(jsonResp.toString());
      resp.setStatus(HttpServletResponse.SC_OK);
    } catch (JSONException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      throw new IOException("Malformed request");
    }
  }
}
