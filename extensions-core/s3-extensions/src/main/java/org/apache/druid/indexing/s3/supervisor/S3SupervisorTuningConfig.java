/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.s3.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;

public class S3SupervisorTuningConfig extends IndexTask.IndexTuningConfig
{
  private final Integer workerThreads;
  private final Integer chatThreads;
  private final Long chatRetries;
  private final Duration httpTimeout;
  private final Duration shutdownTimeout;

  public S3SupervisorTuningConfig(
      @JsonProperty("targetPartitionSize") @Nullable Integer targetPartitionSize,
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
      @JsonProperty("rowFlushBoundary") @Nullable Integer rowFlushBoundary_forBackCompatibility, // DEPRECATED
      @JsonProperty("numShards") @Nullable Integer numShards,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      // This parameter is left for compatibility when reading existing JSONs, to be removed in Druid 0.12.
      @JsonProperty("buildV9Directly") @Nullable Boolean buildV9Directly,
      @JsonProperty("forceExtendableShardSpecs") @Nullable Boolean forceExtendableShardSpecs,
      @JsonProperty("forceGuaranteedRollup") @Nullable Boolean forceGuaranteedRollup,
      @Deprecated @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
      @JsonProperty("publishTimeout") @Nullable Long publishTimeout, // deprecated
      @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable
          SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
      @JsonProperty("workerThreads") Integer workerThreads,
      @JsonProperty("chatThreads") Integer chatThreads,
      @JsonProperty("chatRetries") Long chatRetries,
      @JsonProperty("httpTimeout") Period httpTimeout,
      @JsonProperty("shutdownTimeout") Period shutdownTimeout
  )
  {
    super(
        targetPartitionSize,
        maxRowsInMemory,
        maxBytesInMemory,
        maxTotalRows,
        rowFlushBoundary_forBackCompatibility,
        numShards,
        indexSpec,
        maxPendingPersists,
        buildV9Directly,
        forceExtendableShardSpecs,
        forceGuaranteedRollup,
        reportParseExceptions,
        publishTimeout,
        pushTimeout,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );

    this.workerThreads = workerThreads;
    this.chatThreads = chatThreads;
    this.chatRetries = (chatRetries != null ? chatRetries : 8);
    this.httpTimeout = defaultDuration(httpTimeout, "PT10S");
    this.shutdownTimeout = defaultDuration(shutdownTimeout, "PT80S");
  }

  @JsonProperty
  public Integer getWorkerThreads()
  {
    return workerThreads;
  }

  @JsonProperty
  public Integer getChatThreads()
  {
    return chatThreads;
  }

  @JsonProperty
  public Long getChatRetries()
  {
    return chatRetries;
  }

  @JsonProperty
  public Duration getHttpTimeout()
  {
    return httpTimeout;
  }

  @JsonProperty
  public Duration getShutdownTimeout()
  {
    return shutdownTimeout;
  }

  private static Duration defaultDuration(final Period period, final String theDefault)
  {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }

  @Override
  public String toString()
  {
    return "S3SupervisorTuningConfig{" +
           "workerThreads=" + workerThreads +
           ", chatThreads=" + chatThreads +
           ", chatRetries=" + chatRetries +
           ", httpTimeout=" + httpTimeout +
           ", shutdownTimeout=" + shutdownTimeout +
           '}';
  }
}
