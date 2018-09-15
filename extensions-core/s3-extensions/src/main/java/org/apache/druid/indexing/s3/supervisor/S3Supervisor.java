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

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.firehose.s3.StaticS3FirehoseFactory;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.s3.SupervisedObject;
import org.apache.druid.indexing.overlord.s3.SupervisedObjectInterval;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.parsers.Parser;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Supervisor responsible for managing the KafkaIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link S3SupervisorSpec} which includes the Kafka topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Kafka topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Kafka offsets.
 */
public class S3Supervisor implements Supervisor
{
  private static final EmittingLogger log = new EmittingLogger(S3Supervisor.class);
  private static final Random RANDOM = new Random();
  private static final long MAX_RUN_FREQUENCY_MILLIS = 1000; // prevent us from running too often in response to events
  private static final long NOT_SET = -1;
  private static final long MINIMUM_FUTURE_TIMEOUT_IN_SECONDS = 120;

  private static final String IS_SUPERVISED_CONTEXT_KEY = "isSupervised";
  private static final String AFFECTED_PATHS_CONTEXT_KEY = "affectedPaths";


  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final ObjectMapper objectMapper;
  private final ObjectMapper sortingMapper;
  private final S3SupervisorSpec spec;
  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final String dataSource;
  private final S3SupervisorIOConfig ioConfig;
  private final S3SupervisorTuningConfig tuningConfig;
  private final IndexTask.IndexTuningConfig taskTuningConfig;
  private final String supervisorId;
  private final TaskInfoProvider taskInfoProvider;
  private final long futureTimeoutInSeconds; // how long to wait for async operations to complete
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final ServerSideEncryptingAmazonS3 s3Client;

  private final ExecutorService exec;
  private final ScheduledExecutorService scheduledExec;
  private final ScheduledExecutorService reportingExec;
  private final ListeningExecutorService workerExec;
  private final BlockingQueue<Notice> notices = new LinkedBlockingDeque<>();
  private final Object stopLock = new Object();
  private final Object stateChangeLock = new Object();
  private final Object consumerLock = new Object();

  private boolean listenerRegistered = false;
  private long lastRunTime;

  private volatile DateTime firstRunTime;
  private volatile boolean started = false;
  private volatile boolean stopped = false;

  public S3Supervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final ObjectMapper mapper,
      final S3SupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory,
      final ServerSideEncryptingAmazonS3 s3Client
      )
  {
    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.objectMapper = mapper;
    this.sortingMapper = mapper.copy().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    this.spec = spec;
    this.emitter = spec.getEmitter();
    this.monitorSchedulerConfig = spec.getMonitorSchedulerConfig();
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.s3Client = s3Client;

    this.dataSource = spec.getDataSchema().getDataSource();
    this.ioConfig = spec.getIoConfig();
    this.tuningConfig = spec.getTuningConfig();
    this.taskTuningConfig = IndexTask.IndexTuningConfig.copyOf(this.tuningConfig);
    this.supervisorId = StringUtils.format("KafkaSupervisor-%s", dataSource);
    this.exec = Execs.singleThreaded(supervisorId);
    this.scheduledExec = Execs.scheduledSingleThreaded(supervisorId + "-Scheduler-%d");
    this.reportingExec = Execs.scheduledSingleThreaded(supervisorId + "-Reporting-%d");

    int workerThreads = this.tuningConfig.getWorkerThreads() != null ? this.tuningConfig.getWorkerThreads() : 10;
    this.workerExec = MoreExecutors.listeningDecorator(Execs.multiThreaded(workerThreads, supervisorId + "-Worker-%d"));
    log.info("Created worker pool with [%d] threads for dataSource [%s]", workerThreads, this.dataSource);

    this.taskInfoProvider = new TaskInfoProvider()
    {
      @Override
      public TaskLocation getTaskLocation(final String id)
      {
        Preconditions.checkNotNull(id, "id");
        Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
        if (taskRunner.isPresent()) {
          Optional<? extends TaskRunnerWorkItem> item = Iterables.tryFind(
              taskRunner.get().getRunningTasks(), new Predicate<TaskRunnerWorkItem>()
              {
                @Override
                public boolean apply(TaskRunnerWorkItem taskRunnerWorkItem)
                {
                  return id.equals(taskRunnerWorkItem.getTaskId());
                }
              }
          );

          if (item.isPresent()) {
            return item.get().getLocation();
          }
        } else {
          log.error("Failed to get task runner because I'm not the leader!");
        }

        return TaskLocation.unknown();
      }

      @Override
      public Optional<TaskStatus> getTaskStatus(String id)
      {
        return taskStorage.getStatus(id);
      }
    };

    this.futureTimeoutInSeconds = MINIMUM_FUTURE_TIMEOUT_IN_SECONDS;
  }

  @Override
  public void start()
  {
    synchronized (stateChangeLock) {
      Preconditions.checkState(!started, "already started");
      Preconditions.checkState(!exec.isShutdown(), "already stopped");

      try {
        exec.submit(
            new Runnable()
            {
              @Override
              public void run()
              {
                try {
                  while (!Thread.currentThread().isInterrupted()) {
                    final Notice notice = notices.take();

                    try {
                      notice.handle();
                    }
                    catch (Throwable e) {
                      log.makeAlert(e, "S3Supervisor[%s] failed to handle notice", dataSource)
                         .addData("noticeClass", notice.getClass().getSimpleName())
                         .emit();
                    }
                  }
                }
                catch (InterruptedException e) {
                  log.info("S3Supervisor[%s] interrupted, exiting", dataSource);
                }
              }
            }
        );
        firstRunTime = DateTimes.nowUtc().plus(ioConfig.getStartDelay());
        scheduledExec.scheduleAtFixedRate(
            buildRunTask(),
            ioConfig.getStartDelay().getMillis(),
            Math.max(ioConfig.getPeriod().getMillis(), MAX_RUN_FREQUENCY_MILLIS),
            TimeUnit.MILLISECONDS
        );

//        reportingExec.scheduleAtFixedRate(
//            updateCurrentAndLatestOffsets(),
//            ioConfig.getStartDelay().getMillis() + INITIAL_GET_OFFSET_DELAY_MILLIS, // wait for tasks to start up
//            Math.max(
//                tuningConfig.getOffsetFetchPeriod().getMillis(), MINIMUM_GET_OFFSET_PERIOD_MILLIS
//            ),
//            TimeUnit.MILLISECONDS
//        );
//
//        reportingExec.scheduleAtFixedRate(
//            emitLag(),
//            ioConfig.getStartDelay().getMillis() + INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS, // wait for tasks to start up
//            monitorSchedulerConfig.getEmitterPeriod().getMillis(),
//            TimeUnit.MILLISECONDS
//        );

        started = true;
        log.info(
            "Started S3Supervisor[%s], first run in [%s], with spec: [%s]",
            dataSource,
            ioConfig.getStartDelay(),
            spec.toString()
        );
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception starting S3Supervisor[%s]", dataSource)
           .emit();
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public void stop(boolean stopGracefully)
  {
    synchronized (stateChangeLock) {
      Preconditions.checkState(started, "not started");

      log.info("Beginning shutdown of S3Supervisor[%s]", dataSource);

      try {
        scheduledExec.shutdownNow(); // stop recurring executions
//        reportingExec.shutdownNow();

        Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
        if (taskRunner.isPresent()) {
          taskRunner.get().unregisterListener(supervisorId);
        }

        // Stopping gracefully will synchronize the end offsets of the tasks and signal them to publish, and will block
        // until the tasks have acknowledged or timed out. We want this behavior when we're explicitly shut down through
        // the API, but if we shut down for other reasons (e.g. we lose leadership) we want to just stop and leave the
        // tasks as they are.
        synchronized (stopLock) {
//          if (stopGracefully) {
//            log.info("Posting GracefulShutdownNotice, signalling managed tasks to complete and publish");
//            notices.add(new GracefulShutdownNotice());
//          } else {
            log.info("Posting ShutdownNotice");
            notices.add(new ShutdownNotice());
//          }

          long shutdownTimeoutMillis = tuningConfig.getShutdownTimeout().getMillis();
          long endTime = System.currentTimeMillis() + shutdownTimeoutMillis;
          while (!stopped) {
            long sleepTime = endTime - System.currentTimeMillis();
            if (sleepTime <= 0) {
              log.info("Timed out while waiting for shutdown (timeout [%,dms])", shutdownTimeoutMillis);
              stopped = true;
              break;
            }
            stopLock.wait(sleepTime);
          }
        }
        log.info("Shutdown notice handled");

        workerExec.shutdownNow();
        exec.shutdownNow();
        started = false;

        log.info("S3Supervisor[%s] has stopped", dataSource);
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception stopping S3Supervisor[%s]", dataSource)
           .emit();
      }
    }
  }

  @Override
  public SupervisorReport getStatus()
  {
    return null;
  }

  @Override
  public Map<String, Map<String, Object>> getStats()
  {
    return null;
  }

  @Override
  public void reset(DataSourceMetadata dataSourceMetadata)
  {
    log.info("Posting ResetNotice");
    notices.add(new ResetNotice(dataSourceMetadata));
  }

  @Override
  public void checkpoint(
      @Nullable Integer taskGroupId,
      @Deprecated String baseSequenceName,
      DataSourceMetadata previousCheckPoint,
      DataSourceMetadata currentCheckPoint
  )
  {
    // do nothing
  }

  public void possiblyRegisterListener()
  {
    // getTaskRunner() sometimes fails if the task queue is still being initialized so retry later until we succeed

    if (listenerRegistered) {
      return;
    }

    Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
    if (taskRunner.isPresent()) {
      taskRunner.get().registerListener(
          new TaskRunnerListener()
          {
            @Override
            public String getListenerId()
            {
              return supervisorId;
            }

            @Override
            public void locationChanged(final String taskId, final TaskLocation newLocation)
            {
              // do nothing
            }

            @Override
            public void statusChanged(String taskId, TaskStatus status)
            {
              notices.add(new RunNotice());
            }
          }, MoreExecutors.sameThreadExecutor()
      );

      listenerRegistered = true;
    }
  }

  private interface Notice
  {
    void handle() throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException;
  }

  private class RunNotice implements Notice
  {
    @Override
    public void handle()
    {
      long nowTime = System.currentTimeMillis();
      if (nowTime - lastRunTime < MAX_RUN_FREQUENCY_MILLIS) {
        return;
      }
      lastRunTime = nowTime;

      runInternal();
    }
  }

//  private class GracefulShutdownNotice extends ShutdownNotice
//  {
//    @Override
//    public void handle() throws InterruptedException, ExecutionException, TimeoutException
//    {
//      gracefulShutdownInternal();
//      super.handle();
//    }
//  }

  private class ShutdownNotice implements Notice
  {
    @Override
    public void handle()
    {
      synchronized (stopLock) {
        stopped = true;
        stopLock.notifyAll();
      }
    }
  }

  private class ResetNotice implements Notice
  {
    final DataSourceMetadata dataSourceMetadata;

    ResetNotice(DataSourceMetadata dataSourceMetadata)
    {
      this.dataSourceMetadata = dataSourceMetadata;
    }

    @Override
    public void handle()
    {
      // TODO
      // resetInternal(dataSourceMetadata);
    }
  }

  private Map<String, SupervisedObject> convertSupervisedObjectListToMap(List<SupervisedObject> supervisedObjects)
  {
    if (supervisedObjects == null) {
      return ImmutableMap.of();
    }

    return supervisedObjects.stream().collect(Collectors.toMap(SupervisedObject::getPath, v -> v));
  }

  private String getPath(S3ObjectSummary s3ObjectSummary)
  {
    if (s3ObjectSummary == null) {
      return null;
    }

    return String.format("%s/%s", s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey());
  }

  private void getChanges(Map<String, SupervisedObject> newObjects, Map<String, SupervisedObject> modifiedObjects, Map<String, SupervisedObject> deletedObjects)
  {
    List<S3ObjectSummary> s3Objects = fetchObjects();

    Map<String, SupervisedObject> supervisedObjects = convertSupervisedObjectListToMap(
        indexerMetadataStorageCoordinator.getSupervisedObjects(dataSource));

    for (S3ObjectSummary s3ObjectSummary : s3Objects) {
      String path = getPath(s3ObjectSummary);

      if (!supervisedObjects.containsKey(path)) {
        log.info("OBJ ADD: %s null -> %s", path, s3ObjectSummary.getETag());
        newObjects.put(path, supervisedObjectFromS3ObjectSummary(s3ObjectSummary));
      } else {
        SupervisedObject trackedObject = supervisedObjects.get(path);
        if (!s3ObjectSummary.getETag().equals(trackedObject.getHash())) {
          log.info("OBJ MOD: %s %s -> %s", path, trackedObject.getHash(), s3ObjectSummary.getETag());
          modifiedObjects.put(path, supervisedObjectFromS3ObjectSummary(s3ObjectSummary));
        } else {
          log.info("OBJ ---: %s %s -> %s", path, trackedObject.getHash(), s3ObjectSummary.getETag());
        }
      }

      supervisedObjects.remove(path);
    }

    if (!supervisedObjects.isEmpty()) {
      supervisedObjects.values().forEach(x -> log.info("OBJ DEL: %s %s -> null", x.getPath(), x.getHash()));
      deletedObjects.putAll(supervisedObjects.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().withNullHash())));
    }
  }

  private static String uriToPath(URI uri)
  {
    return uri == null ? null : String.format("%s%s", uri.getHost(), uri.getPath());
  }

  private static URI pathToS3Uri(String path)
  {
    try {
      return new URI(String.format("s3://%s", path));
    }
    catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
  }

  private SupervisedObject supervisedObjectFromS3ObjectSummary(S3ObjectSummary s3ObjectSummary)
  {
    return new SupervisedObject(
        dataSource,
        getPath(s3ObjectSummary),
        new DateTime(s3ObjectSummary.getLastModified()),
        0L,
        s3ObjectSummary.getETag()
    );
  }

  @VisibleForTesting
  void runInternal()
  {
    log.info("---------- START RUN ----------");

    Map<String, SupervisedObject> newObjects = new HashMap<>();
    Map<String, SupervisedObject> modifiedObjects = new HashMap<>();
    Map<String, SupervisedObject> deletedObjects = new HashMap<>();

    getChanges(newObjects, modifiedObjects, deletedObjects);

    List<IndexTask> supervisedTasks = discoverTasks();

    List<String> activePaths = new ArrayList<>();
    for (IndexTask task : supervisedTasks) {
      List<String> affectedPaths = (List<String>) task.getContext()
                                                      .getOrDefault(AFFECTED_PATHS_CONTEXT_KEY, ImmutableList.of());

      activePaths.addAll(affectedPaths);
    }

    // Remove objects that are already being processed by a running task
    activePaths.forEach(x -> {
      newObjects.remove(x);
      modifiedObjects.remove(x);
      deletedObjects.remove(x);
    });

    // TODO: Make sure add and modified tasks play nicely with each other since one is append and one is overwrite
    // This is a pretty simplistic operation for a PoC that assumes that only one thing will change at a time.

    addTasksForNewObjects(newObjects);

    // TODO: Right now treat deleted objects as modified, even though there's a special case when no other file contains
    // data in the interval and the existing segment should just be dropped or overwritten with an empty segment.
    modifiedObjects.putAll(deletedObjects);

    addTasksForModifiedObjects(modifiedObjects);

    log.info("----------  END RUN  ----------");
  }

  private void addTasksForNewObjects(Map<String, SupervisedObject> objectMap)
  {
    if (objectMap == null || objectMap.isEmpty()) {
      return;
    }

    List<SupervisedObjectInterval> supervisedObjectIntervals = new ArrayList<>();

    objectMap.keySet()
             .forEach(x -> supervisedObjectIntervals.addAll(intervalHashMapToSupervisedObjectIntervalList(
                 calculateIntervalHashes(getInputStreamForS3Object(x)), x)));

    // TODO: Well this is mighty ugly
    createTask(
        objectMap.keySet().stream().map(S3Supervisor::pathToS3Uri).collect(Collectors.toList()),
        new ArrayList<>(objectMap.keySet()),
        null,
        new ArrayList<>(objectMap.values()),
        supervisedObjectIntervals,
        true
    );
  }

  private List<SupervisedObjectInterval> intervalHashMapToSupervisedObjectIntervalList(Map<Interval, String> intervalHashes, String path)
  {
    if (intervalHashes == null) {
      return ImmutableList.of();
    }

    return intervalHashes.entrySet()
                         .stream()
                         .map(x -> new SupervisedObjectInterval(dataSource, path, x.getKey(), x.getValue()))
                         .collect(Collectors.toList());
  }

  private Map<Interval, String> supervisedObjectIntervalListToIntervalHashMap(List<SupervisedObjectInterval> supervisedObjectIntervals)
  {
    if (supervisedObjectIntervals == null) {
      return ImmutableMap.of();
    }

    return supervisedObjectIntervals.stream()
                                    .collect(Collectors.toMap(
                                        SupervisedObjectInterval::getInterval,
                                        SupervisedObjectInterval::getHash
                                    ));
  }

  private void addTasksForModifiedObjects(Map<String, SupervisedObject> objectMap)
  {
//    log.info("Modified paths: %s", objectMap.keySet());

    Set<String> pathsToIndex = new HashSet<>();
    Set<Interval> allChangedIntervals = new HashSet<>();
    List<SupervisedObjectInterval> allSupervisedObjectIntervals = new ArrayList<>();

    for (Map.Entry<String, SupervisedObject> objEntry : objectMap.entrySet()) {

      String path = objEntry.getKey();
      boolean wasDeleted = objEntry.getValue().getHash() == null;

      // 1) scan the file to find the intervals and their hashes
      Map<Interval, String> intervalHashes = wasDeleted
                                             ? ImmutableMap.of()
                                             : calculateIntervalHashes(getInputStreamForS3Object(path));

      // 2) compare with the hashes generated the last time we indexing this file to figure out what intervals changed
      Map<Interval, String> previousIntervals = supervisedObjectIntervalListToIntervalHashMap(
          indexerMetadataStorageCoordinator.getSupervisedObjectIntervalsForPath(dataSource, path));

      List<Interval> changedIntervals = new ArrayList<>();

      for (Map.Entry<Interval, String> entry : intervalHashes.entrySet()) {
        if (!previousIntervals.containsKey(entry.getKey())) {
          log.info("IVL ADD: %s [%s] %s -> %s", entry.getKey(), path, "null", entry.getValue());
          changedIntervals.add(entry.getKey());
          allSupervisedObjectIntervals.add(new SupervisedObjectInterval(dataSource, path, entry.getKey(), entry.getValue()));
          continue;
        }

        if (!entry.getValue().equals(previousIntervals.get(entry.getKey()))) {
          log.info("IVL MOD: %s [%s] %s -> %s", entry.getKey(), path, previousIntervals.get(entry.getKey()), entry.getValue());
          changedIntervals.add(entry.getKey());
          allSupervisedObjectIntervals.add(new SupervisedObjectInterval(dataSource, path, entry.getKey(), entry.getValue()));
          continue;
        }

        log.info("IVL ---: %s [%s] %s -> %s", entry.getKey(), path, entry.getValue(), entry.getValue());
      }

      // get deleted intervals
      intervalHashes.keySet().forEach(previousIntervals::remove);
      if (!previousIntervals.isEmpty()) {
        previousIntervals.keySet().forEach(x -> {
          log.info("IVL DEL: %s [%s] %s -> %s", x, path, previousIntervals.get(x), "null");
          allSupervisedObjectIntervals.add(new SupervisedObjectInterval(dataSource, path, x, null));
        });
        changedIntervals.addAll(previousIntervals.keySet());
      }

      // 3) for each interval, find all files that also includes the same interval (includes ourself)
      for (Interval interval : changedIntervals) {
        List<SupervisedObjectInterval> supervisedObjectIntervals = indexerMetadataStorageCoordinator.getSupervisedObjectIntervalsWithInterval(
            dataSource,
            interval
        );

        // 4) add these to our list of paths that need indexing
        supervisedObjectIntervals.forEach(x -> pathsToIndex.add(x.getPath()));
      }

      // 5) remove ourself from the list if we were deleted
      if (wasDeleted) {
        pathsToIndex.remove(path);
      }

      allChangedIntervals.addAll(changedIntervals);
    }

    if (!pathsToIndex.isEmpty()) {
      log.info("Paths to index: %s", pathsToIndex);

      // TODO: Well this is mighty ugly
      createTask(
          pathsToIndex.stream().map(S3Supervisor::pathToS3Uri).collect(Collectors.toList()),
          new ArrayList<>(objectMap.keySet()),
          new ArrayList<>(allChangedIntervals),
          new ArrayList<>(objectMap.values()),
          allSupervisedObjectIntervals,
          false // modifications must do an overwrite
      );
    }
  }

  private InputStream getInputStreamForS3Object(String path)
  {
    // TODO: Yuck
    String[] fragments = path.split("/", 2);

    return s3Client.getObject(fragments[0], fragments[1]).getObjectContent();
  }

  private Map<Interval, String> calculateIntervalHashes(InputStream is)
  {
    Parser<String, Object> parser = spec.getDataSchema().getParser().getParseSpec().makeParser();
    Granularity segmentGranularity = spec.getDataSchema().getGranularitySpec().getSegmentGranularity();

    Map<Interval, String> intervalHashes = new HashMap<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(is)))
    {
      while(true)
      {
        String line = reader.readLine();
        if (line == null) {
          break;
        }

        DateTime timestamp = spec.getDataSchema()
                                 .getParser()
                                 .getParseSpec()
                                 .getTimestampSpec()
                                 .extractTimestamp(parser.parseToMap(line));

        intervalHashes.compute(
            segmentGranularity.bucket(timestamp),
            (k, v) -> v == null ? DigestUtils.md5Hex(line) : DigestUtils.md5Hex(v + line)
        );
      }
    }
    catch (IOException e) {
      Throwables.propagate(e);
    }

    return intervalHashes;
  }



  // size, lastModified, do suffix filtering?
  private List<S3ObjectSummary> fetchObjects() {
    List<S3ObjectSummary> objectSummaries = new ArrayList<>();

    String continuationToken = null;
    while(true)
    {
      ListObjectsV2Request request = new ListObjectsV2Request().withBucketName("imply-scratch").withPrefix("dave/supervisor");

      if (continuationToken != null) {
        request.withContinuationToken(continuationToken);
      }

      ListObjectsV2Result result = s3Client.listObjectsV2(request);

      if (result.getObjectSummaries() != null) {
        objectSummaries.addAll(result.getObjectSummaries());
      }
      if (!result.isTruncated()) {
        break;
      }

      continuationToken = result.getNextContinuationToken();
    }

    return objectSummaries.stream().filter(x -> x.getSize() != 0).collect(Collectors.toList());
  }

  private List<IndexTask> discoverTasks()
  {
    List<IndexTask> supervisedTasks = new ArrayList<>();
    List<Task> tasks = taskStorage.getActiveTasks();

    for (Task task : tasks) {
      if (!(task instanceof IndexTask) || !dataSource.equals(task.getDataSource())) {
        continue;
      }

      Map<String, Object> context = task.getContext();
      if (context == null || !context.containsKey(IS_SUPERVISED_CONTEXT_KEY) || !((boolean) context.get(
          IS_SUPERVISED_CONTEXT_KEY))) {
        continue;
      }

      supervisedTasks.add((IndexTask)task);
    }

    log.info("Found [%d] managed indexing tasks for dataSource [%s]", supervisedTasks.size(), dataSource);

    return supervisedTasks;
  }

  private void createTask(
      List<URI> uris,
      List<String> affectedPaths,
      List<Interval> intervalOverride,
      List<SupervisedObject> supervisedObjects,
      List<SupervisedObjectInterval> supervisedObjectIntervals,
      boolean appendToExisting
  )
  {
    String id = "davidsTask_" + UUID.randomUUID().toString();

    Map<String, Object> context = null;
    try {
      context = ImmutableMap.of(IS_SUPERVISED_CONTEXT_KEY, true,
                                AFFECTED_PATHS_CONTEXT_KEY, affectedPaths,
                                SupervisedObject.SUPERVISED_OBJECTS_CONTEXT_KEY, objectMapper.writeValueAsString(supervisedObjects),
                                SupervisedObjectInterval.SUPERVISED_OBJECT_INTERVALS_CONTEXT_KEY, objectMapper.writeValueAsString(supervisedObjectIntervals)
      );
    }
    catch (JsonProcessingException e) {
      Throwables.propagate(e);
    }

    IndexTask.IndexIOConfig ioConfig = new IndexTask.IndexIOConfig(new StaticS3FirehoseFactory(
        s3Client, uris, null, null, null, null, null, null), appendToExisting);

    DataSchema dataSchema = spec.getDataSchema();
    if (intervalOverride != null && !intervalOverride.isEmpty()) {
      dataSchema = new DataSchema(
          dataSchema.getDataSource(),
          dataSchema.getParserMap(),
          dataSchema.getAggregators(),
          dataSchema.getGranularitySpec().withIntervals(intervalOverride),
          dataSchema.getTransformSpec(),
          objectMapper
      );
    }

    IndexTask task = new IndexTask(
        id,
        new TaskResource(id, 1),
        new IndexTask.IndexIngestionSpec(dataSchema, ioConfig, taskTuningConfig),
        context,
        null,
        null,
        rowIngestionMetersFactory
    );

    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      try {
        log.info("Adding taskId [%s] to process paths: %s", id, affectedPaths);
        taskQueue.get().add(task);
      }
      catch (EntryExistsException e) {
        log.error("Tried to add task [%s] but it already exists", task.getId());
      }
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  private void killTask(final String id)
  {
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      taskQueue.get().shutdown(id);
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  private Runnable buildRunTask()
  {
    return () -> notices.add(new RunNotice());
  }


}
