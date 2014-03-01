package com.bluelake.datamodule;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;

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
    
    DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
    RingBufferEntity entity = new RingBufferEntity(kind, key, 6);
    JSONArray result = new JSONArray();
    for (JSONObject obj : entity) {
      result.put(obj);
    }
    resp.setContentType("application/json");
    resp.getWriter().print(result.toString());
    resp.setStatus(HttpServletResponse.SC_OK);
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

  }
}
