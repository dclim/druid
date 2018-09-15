package org.apache.druid.indexing.overlord.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

public class SupervisedObjectInterval
{
  public static final String SUPERVISED_OBJECT_INTERVALS_CONTEXT_KEY = "supervisedObjectIntervals";

  private final String dataSource;
  private final String path;
  private final Interval interval;
  private final String hash;

  @JsonCreator
  public SupervisedObjectInterval(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("path") String path,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("hash") String hash
  )
  {
    this.dataSource = dataSource;
    this.path = path;
    this.interval = interval;
    this.hash = hash;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public String getPath()
  {
    return path;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public String getHash()
  {
    return hash;
  }
}
