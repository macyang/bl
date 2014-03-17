package com.bluelake.datahub;

public class DataHubConstants {
  public static final String PASSPORT_KIND = "passport";
  public static final String ENTITY_KIND = "entity";
  public static final String INGEST_QUEUE = "ingestqueue";
  // smaller max back off, used to check BQ job status
  public static final String BQ_QUEUE = "bqqueue";
  public static final String POLLJOB_URL = "/polljob";
  public static final String GCSINGEST_URL = "/ingest";
  public static final String BQINGEST_URL = "/bqingest";
  public static final long GCS_SPLITSIZE = 10;

  public static final String INPUT_REQUEST_PARAM_BQTABLE = "bqtable";
  public static final String INPUT_REQUEST_PARAM_BUCKET = "bucket";
  public static final String INPUT_REQUEST_PARAM_ENTITYKEY = "entitykey";
  public static final String INPUT_REQUEST_PARAM_GCSOBJS = "gcsobjs";

  public static final String INGEST_REQUEST_PARAM_GCSOBJ = "gcsobj";
  public static final String INGEST_REQUEST_PARAM_STARTINDEX = "startindex";
  public static final String INGEST_REQUEST_PARAM_PAGESIZE = "pagesize";
  public static final String INGEST_REQUEST_PARAM_PROJECTID = "projectid";
  public static final String INGEST_REQUEST_PARAM_JOBID = "jobid";
  public static final String INGEST_REQUEST_PARAM_STARTTIME = "starttime";
  public static final String JOBOBJECT = "jobobj";
  public static final String PROP_CONF = "blconf";


}
