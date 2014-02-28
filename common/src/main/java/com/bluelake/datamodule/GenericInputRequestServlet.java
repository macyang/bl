package com.bluelake.datamodule;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

public class GenericInputRequestServlet extends HttpServlet {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(GenericInputRequestServlet.class.getName());

  /*
   * Expect the body of the post to be valid JSON
   * 
   * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
   */
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    JSONObject jsonObject = GcpUtil.getJSONRequest(req);
    JSONObject data = null;
    try {
      data = jsonObject.getJSONObject("data");
      LOG.log(Level.INFO, "data : " + data.toString());
    } catch (JSONException e) {
      throw new IOException("error getting query parameter from JSON");
    }
    
    RingBufferEntity ringBuffer = new RingBufferEntity("ringbufferentity", "testkey", 3);
    ringBuffer.store(data); 
    ringBuffer.put();
  }
}
