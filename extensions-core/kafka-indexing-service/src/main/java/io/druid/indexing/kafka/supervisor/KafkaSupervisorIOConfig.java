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

package io.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.util.HashMap;
import java.util.Map;

public class KafkaSupervisorIOConfig
{
  private final String topic;
  private final String kafkaBrokers;
  private final Integer replicas;
  private final Integer taskCount;
  private final Duration taskDuration;
  private final Map<String, String> consumerProperties;
  private final Duration startDelay;
  private final Duration period;
  private final Boolean useEarliestOffset;
  private final Duration completionTimeout;

  @JsonCreator
  public KafkaSupervisorIOConfig(
      @JsonProperty("topic") String topic,
      @JsonProperty("kafkaBrokers") String kafkaBrokers,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("taskCount") Integer taskCount,
      @JsonProperty("taskDuration") Period taskDuration,
      @JsonProperty("consumerProperties") Map<String, String> consumerProperties,
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period,
      @JsonProperty("useEarliestOffset") Boolean useEarliestOffset,
      @JsonProperty("completionTimeout") Period completionTimeout
  )
  {
    this.topic = Preconditions.checkNotNull(topic, "topic");
    this.kafkaBrokers = Preconditions.checkNotNull(kafkaBrokers, "kafkaBrokers");
    this.replicas = (replicas != null ? replicas : 1);
    this.taskCount = (taskCount != null ? taskCount : 1);
    this.taskDuration = defaultDuration(taskDuration, "PT1H");
    this.consumerProperties = (consumerProperties != null ? consumerProperties : new HashMap<String, String>());
    this.startDelay = defaultDuration(startDelay, "PT5S");
    this.period = defaultDuration(period, "PT30S");
    this.useEarliestOffset = (useEarliestOffset != null ? useEarliestOffset : false);
    this.completionTimeout = defaultDuration(completionTimeout, "PT30M");
  }

  @JsonProperty
  public String getTopic()
  {
    return topic;
  }

  @JsonProperty
  public String getKafkaBrokers()
  {
    return kafkaBrokers;
  }

  @JsonProperty
  public Integer getReplicas()
  {
    return replicas;
  }

  @JsonProperty
  public Integer getTaskCount()
  {
    return taskCount;
  }

  @JsonProperty
  public Duration getTaskDuration()
  {
    return taskDuration;
  }

  @JsonProperty
  public Map<String, String> getConsumerProperties()
  {
    return consumerProperties;
  }

  @JsonProperty
  public Duration getStartDelay()
  {
    return startDelay;
  }

  @JsonProperty
  public Duration getPeriod()
  {
    return period;
  }

  @JsonProperty
  public Boolean isUseEarliestOffset()
  {
    return useEarliestOffset;
  }

  @JsonProperty
  public Duration getCompletionTimeout()
  {
    return completionTimeout;
  }

  private static Duration defaultDuration(final Period period, final String theDefault)
  {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }
}