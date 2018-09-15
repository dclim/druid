package org.apache.druid.indexing.overlord.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

public class SupervisedObject
{
  public static final String SUPERVISED_OBJECTS_CONTEXT_KEY = "supervisedObjects";
  
  private final String dataSource;
  private final String path;
  private final DateTime lastModified;
  private final Long numRows;
  private final String hash;

  @JsonCreator
  public SupervisedObject(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("path") String path,
      @JsonProperty("lastModified") DateTime lastModified,
      @JsonProperty("numRows") Long numRows,
      @JsonProperty("hash") String hash
  )
  {
    this.dataSource = dataSource;
    this.path = path;
    this.lastModified = lastModified;
    this.numRows = numRows;
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
  public DateTime getLastModified()
  {
    return lastModified;
  }

  @JsonProperty
  public Long getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public String getHash()
  {
    return hash;
  }

  public SupervisedObject withNullHash()
  {
    return new SupervisedObject(dataSource, path, lastModified, numRows, null);
  }
}
