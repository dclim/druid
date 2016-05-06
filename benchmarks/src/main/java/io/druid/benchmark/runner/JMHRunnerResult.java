/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.runner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.openjdk.jmh.annotations.Mode;

import java.util.concurrent.TimeUnit;

public class JMHRunnerResult implements BaselineEntry
{
  private DateTime timestamp;
  private String id;
  private String benchmark;
  private Mode mode;
  private TimeUnit timeUnit;
  private Long sampleCount;
  private Double score;
  private String scoreUnit;
  private Double scoreError;
  private Double errorRatio;
  private Double p50;
  private Double p90;
  private Double p95;
  private Double p99;
  private Boolean baseline;

  @JsonCreator
  public JMHRunnerResult(
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("id") String id,
      @JsonProperty("benchmark") String benchmark,
      @JsonProperty("mode") Mode mode,
      @JsonProperty("timeUnit") TimeUnit timeUnit,
      @JsonProperty("sampleCount") Long sampleCount,
      @JsonProperty("score") Double score,
      @JsonProperty("scoreUnit") String scoreUnit,
      @JsonProperty("scoreError") Double scoreError,
      @JsonProperty("errorRatio") Double errorRatio,
      @JsonProperty("p50") Double p50,
      @JsonProperty("p90") Double p90,
      @JsonProperty("p95") Double p95,
      @JsonProperty("p99") Double p99,
      @JsonProperty("baseline") Boolean baseline
  )
  {
    this.timestamp = timestamp;
    this.id = id;
    this.benchmark = benchmark;
    this.mode = mode;
    this.timeUnit = timeUnit;
    this.sampleCount = sampleCount;
    this.score = score;
    this.scoreUnit = scoreUnit;
    this.scoreError = scoreError;
    this.errorRatio = errorRatio;
    this.p50 = p50;
    this.p90 = p90;
    this.p95 = p95;
    this.p99 = p99;
    this.baseline = baseline;
  }

  @JsonProperty
  @Override
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  @Override
  public String getBenchmark()
  {
    return benchmark;
  }

  @JsonProperty
  @Override
  public Mode getMode()
  {
    return mode;
  }

  @JsonProperty
  @Override
  public TimeUnit getTimeUnit()
  {
    return timeUnit;
  }

  @JsonProperty
  public Long getSampleCount()
  {
    return sampleCount;
  }

  @JsonProperty
  @Override
  public Double getScore()
  {
    return score;
  }

  @JsonProperty
  @Override
  public String getScoreUnit()
  {
    return scoreUnit;
  }

  @JsonProperty
  public Double getScoreError()
  {
    return scoreError;
  }

  @JsonProperty
  public Double getErrorRatio()
  {
    return errorRatio;
  }

  @JsonProperty
  public Double getP50()
  {
    return p50;
  }

  @JsonProperty
  public Double getP90()
  {
    return p90;
  }

  @JsonProperty
  public Double getP95()
  {
    return p95;
  }

  @JsonProperty
  public Double getP99()
  {
    return p99;
  }

  @JsonProperty
  public Boolean isBaseline()
  {
    return baseline;
  }

  @Override
  public String toString()
  {
    return "JMHRunnerResult{" +
           "timestamp=" + timestamp +
           ", id='" + id + '\'' +
           ", benchmark='" + benchmark + '\'' +
           ", mode=" + mode +
           ", timeUnit=" + timeUnit +
           ", sampleCount=" + sampleCount +
           ", score=" + score +
           ", scoreUnit='" + scoreUnit + '\'' +
           ", scoreError=" + scoreError +
           ", errorRatio=" + errorRatio +
           ", p50=" + p50 +
           ", p90=" + p90 +
           ", p95=" + p95 +
           ", p99=" + p99 +
           ", baseline=" + baseline +
           '}';
  }
}
