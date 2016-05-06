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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.benchmark.ConciseComplementBenchmark;
import org.joda.time.DateTime;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.BenchmarkList;
import org.openjdk.jmh.runner.BenchmarkListEntry;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.WorkloadParams;
import org.openjdk.jmh.runner.format.OutputFormatFactory;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public class JMHRunner
{
  private static final Logger log = new Logger(JMHRunner.class);

  public static void main(String[] args) throws Exception
  {
    BaselineReader reader = new JsonFileBaselineReader("testfile.json");
    ResultWriter writer = new JsonFileResultWriter("testfile.json");

    Options opt = new OptionsBuilder()
//        .include(TimeParseBenchmark.class.getSimpleName())
        .include(ConciseComplementBenchmark.class.getSimpleName())
//        .include(".*")
//        .resultFormat(ResultFormatType.JSON)
        .warmupIterations(2)
        .measurementIterations(12)
        .jvmArgs("-server")
        .forks(1)
        .build();


    Map<String, BaselineEntry> baselines = reader.getBaseline();
    SortedSet<BenchmarkListEntry> benchmarkEntries = BenchmarkList.defaultList().find(
        OutputFormatFactory.createFormatInstance(null, VerboseMode.SILENT), opt.getIncludes(), opt.getExcludes()
    );

    for (BenchmarkListEntry entry : benchmarkEntries) {
      List<String> ids = getIds(opt, entry);
      for (String id : ids) {
        if (!baselines.containsKey(id)) {
          throw new ISE("No baseline found for benchmark [%s]; generate a new baseline by specifying TODO", id);
        }

        BaselineEntry baseline = baselines.get(id);
        if (baseline.getTimeUnit() != entry.getTimeUnit().get()) {
          throw new ISE(
              "Baseline timeUnit [%s] does not match configured timeUnit [%s] for benchmark [%s]; generate a new baseline by specifying TODO",
              baseline.getTimeUnit(),
              entry.getTimeUnit().get(),
              id
          );
        }
      }
    }

    Collection<RunResult> results = new Runner(opt).run();

    List<JMHRunnerResult> runnerResults = new ArrayList<>();

    for (RunResult result : results) {
      BenchmarkParams params = result.getParams();
      Result primaryResult = result.getPrimaryResult();

      JMHRunnerResult runnerResult = new JMHRunnerResult(
          DateTime.now(),
          params.id(),
          params.getBenchmark(),
          params.getMode(),
          params.getTimeUnit(),
          primaryResult.getSampleCount(),
          primaryResult.getScore(),
          primaryResult.getScoreUnit(),
          primaryResult.getScoreError(),
          primaryResult.getScoreError() / primaryResult.getScore(),
          primaryResult.getStatistics().getPercentile(50),
          primaryResult.getStatistics().getPercentile(90),
          primaryResult.getStatistics().getPercentile(95),
          primaryResult.getStatistics().getPercentile(99),
          false
      );

      runnerResults.add(runnerResult);
    }

    writer.write(runnerResults);

    final double ERROR_RATIO_THRESHOLD = 0.10;
    final double BASELINE_VARIANCE_THRESHOLD = 0.1;

    List<String> failures = Lists.newArrayList();
    for (JMHRunnerResult result : runnerResults) {
      String id = result.getId();
      Double score = result.getScore();
      Double errorRatio = result.getErrorRatio();
      BaselineEntry baseline = baselines.get(id);

      if (errorRatio > ERROR_RATIO_THRESHOLD) {
        failures.add(
            String.format(
                "Result for [%s] exceeds error threshold: errorRatio [%f] > [%f]",
                id,
                errorRatio,
                ERROR_RATIO_THRESHOLD
            )
        );
      }

      final double baselineTolerance = baseline.getScore() * BASELINE_VARIANCE_THRESHOLD;
      if (Math.abs(score - baseline.getScore()) > baselineTolerance) {
        failures.add(
            String.format(
                "Result for [%s] exceeds baseline threshold: [%f %s] vs baseline [%f %s] (+/- %f %s)",
                id,
                score,
                result.getScoreUnit(),
                baseline.getScore(),
                baseline.getScoreUnit(),
                baselineTolerance,
                baseline.getScoreUnit()
            )
        );
      }
    }

    if (!failures.isEmpty()) {
      throw new RuntimeException("One or more benchmarks failed:\n" + Joiner.on('\n').join(failures));
    }

  }

  // modified from JMH's BenchmarkParams.id()
  private static List<String> getIds(Options options, BenchmarkListEntry entry) throws RunnerException
  {
    List<String> ids = Lists.newArrayList();
    List<WorkloadParams> explodedParams = explodeAllParams(options, entry);

    for (WorkloadParams params : explodedParams) {
      StringBuilder sb = new StringBuilder();
      sb.append(entry.getUsername()).append("-");
      sb.append(entry.getMode());
      for (String key : params.keys()) {
        sb.append("-");
        sb.append(key).append("-").append(params.get(key));
      }
      ids.add(sb.toString());
    }

    return ids;
  }

  // copied from JMH's Runner.explodeAllParams()
  private static List<WorkloadParams> explodeAllParams(Options options, BenchmarkListEntry br) throws RunnerException
  {
    Map<String, String[]> benchParams = br.getParams().orElse(Collections.<String, String[]>emptyMap());
    List<WorkloadParams> ps = new ArrayList<WorkloadParams>();
    for (Map.Entry<String, String[]> e : benchParams.entrySet()) {
      String k = e.getKey();
      String[] vals = e.getValue();
      Collection<String> values = options.getParameter(k).orElse(Arrays.asList(vals));
      if (values.isEmpty()) {
        throw new RunnerException(
            "Benchmark \"" + br.getUsername() +
            "\" defines the parameter \"" + k + "\", but no default values.\n" +
            "Define the default values within the annotation, or provide the parameter values at runtime."
        );
      }
      if (ps.isEmpty()) {
        int idx = 0;
        for (String v : values) {
          WorkloadParams al = new WorkloadParams();
          al.put(k, v, idx);
          ps.add(al);
          idx++;
        }
      } else {
        List<WorkloadParams> newPs = new ArrayList<WorkloadParams>();
        for (WorkloadParams p : ps) {
          int idx = 0;
          for (String v : values) {
            WorkloadParams al = p.copy();
            al.put(k, v, idx);
            newPs.add(al);
            idx++;
          }
        }
        ps = newPs;
      }
    }
    return ps;
  }


}
