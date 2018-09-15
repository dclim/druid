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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;
import org.joda.time.Period;

public class S3SupervisorIOConfig
{
  private final Duration startDelay;
  private final Duration period;

  @JsonCreator
  public S3SupervisorIOConfig(
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period
  )
  {
    this.startDelay = defaultDuration(startDelay, "PT5S");
    this.period = defaultDuration(period, "PT30S");
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

  private static Duration defaultDuration(final Period period, final String theDefault)
  {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }

  @Override
  public String toString()
  {
    return "S3SupervisorIOConfig{" +
           "startDelay=" + startDelay +
           ", period=" + period +
           '}';
  }
}
