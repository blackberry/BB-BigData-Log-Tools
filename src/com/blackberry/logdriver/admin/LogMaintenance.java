/** Copyright (c) 2014 BlackBerry Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. 
 */

package com.blackberry.logdriver.admin;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.logdriver.fs.PathInfo;
import com.blackberry.logdriver.locks.LockInfo;
import com.blackberry.logdriver.locks.LockUtil;
import com.blackberry.logdriver.mapred.avro.AvroBlockInputFormat;
import com.blackberry.logdriver.mapreduce.boom.BoomFilterMapper;

@SuppressWarnings("deprecation")
public class LogMaintenance extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory
      .getLogger(LogMaintenance.class);

  private static enum Status {
    NOT_STARTED, RUNNING, SUCCESS, FAILURE, ERROR
  };

  private final long startTime = System.currentTimeMillis();

  private static final String FILTER_LOCATION = "/oozie/workflows/filterjob/";
  private static final Pattern VALID_FILE = Pattern.compile(".*([0-9]|\\.bm)$");
  private static final String READY_MARKER = "_READY";

  // How long to wait after a directory stops being written to, before we start
  // processing.
  private static final long WAIT_TIME = 10 * 60 * 1000l;

  // Use a single LockUtil instance everywhere
  private LockUtil lockUtil;

  private FileSystem fs;

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    // If run by Oozie, then load the Oozie conf too
    if (System.getProperty("oozie.action.conf.xml") != null) {
      conf.addResource(new URL("file://"
          + System.getProperty("oozie.action.conf.xml")));
    }

    // For some reason, Oozie needs some options to be set in system instead of
    // in the confiuration. So copy the configs over.
    {
      Iterator<Entry<String, String>> i = conf.iterator();
      while (i.hasNext()) {
        Entry<String, String> next = i.next();
        System.setProperty(next.getKey(), next.getValue());
      }
    }

    if (args.length < 3) {
      printUsage();
      return 1;
    }

    String userName = args[0];
    String dcNumber = args[1];
    String service = args[2];
    String date = null;
    String hour = null;
    if (args.length >= 4) {
      date = args[3];
    }
    if (args.length >= 5) {
      hour = args[4];
    }

    // Set from environment variables
    String mergeJobPropertiesFile = getConfOrEnv(conf, "MERGEJOB_CONF");
    String filterJobPropertiesFile = getConfOrEnv(conf, "FILTERJOB_CONF");
    String daysBeforeArchive = getConfOrEnv(conf, "DAYS_BEFORE_ARCHIVE");
    String daysBeforeDelete = getConfOrEnv(conf, "DAYS_BEFORE_DELETE");
    String maxConcurrentMR = getConfOrEnv(conf, "MAX_CONCURRENT_MR", "-1");
    String zkConnectString = getConfOrEnv(conf, "ZK_CONNECT_STRING");
    String logdir = getConfOrEnv(conf, "logdriver.logdir.name");
    boolean resetOrphanedJobs = Boolean.parseBoolean(getConfOrEnv(conf,
        "reset.orphaned.jobs", "true"));
    String rootDir = getConfOrEnv(conf, "service.root.dir");
    String maxTotalMR = getConfOrEnv(conf, "MAX_TOTAL_MR", "-1");

    boolean doMerge = true;
    boolean doArchive = true;
    boolean doDelete = true;

    if (zkConnectString == null) {
      LOG.error("ZK_CONNECT_STRING is not set.  Exiting.");
      return 1;
    }
    if (mergeJobPropertiesFile == null) {
      LOG.info("MERGEJOB_CONF is not set.  Not merging.");
      doMerge = false;
    }
    if (filterJobPropertiesFile == null) {
      LOG.info("FILTERJOB_CONF is not set.  Not archiving.");
      doArchive = false;
    }
    if (daysBeforeArchive == null) {
      LOG.info("DAYS_BEFORE_ARCHIVE is not set.  Not archiving.");
      doArchive = false;
    }
    if (doArchive && Integer.parseInt(daysBeforeArchive) < 0) {
      LOG.info("DAYS_BEFORE_ARCHIVE is negative.  Not archiving.");
      doArchive = false;
    }
    if (daysBeforeDelete == null) {
      LOG.info("DAYS_BEFORE_DELETE is not set.  Not deleting.");
      doDelete = false;
    }
    if (doDelete && Integer.parseInt(daysBeforeDelete) < 0) {
      LOG.info("DAYS_BEFORE_DELETE is negative.  Not deleting.");
      doDelete = false;
    }
    if (logdir == null) {
      LOG.info("LOGDRIVER_LOGDIR_NAME is not set.  Using default value of 'logs'.");
      logdir = "logs";
    }
    if (rootDir == null) {
      LOG.info("SERVICE_ROOT_DIR is not set.  Using default value of 'service'.");
      rootDir = "/service";
    }

    // We can hang if this fails. So make sure we abort if it fails.
    fs = null;
    try {
      fs = FileSystem.get(conf);
      fs.exists(new Path("/")); // Test if it works.
    } catch (IOException e) {
      LOG.error("Error getting filesystem.", e);
      return 1;
    }

    // Create the LockUtil instance
    lockUtil = new LockUtil(zkConnectString);

    // Now it's safe to create our Job Runner
    JobRunner jobRunner = new JobRunner(Integer.parseInt(maxConcurrentMR),
        Integer.parseInt(maxTotalMR));
    Thread jobRunnerThread = new Thread(jobRunner);
    jobRunnerThread.setName("JobRunner");
    jobRunnerThread.setDaemon(false);
    jobRunnerThread.start();

    // Figure out what date we start filters on.
    String filterCutoffDate = "";
    if (doArchive) {
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.DAY_OF_MONTH, Integer.parseInt("-" + daysBeforeArchive));
      filterCutoffDate = String.format("%04d%02d%02d%02d",
          cal.get(Calendar.YEAR), (cal.get(Calendar.MONTH) + 1),
          cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY));
      LOG.info("Archiving logs from before {}", filterCutoffDate);
    }
    String deleteCutoffDate = "";
    if (doDelete) {
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.DAY_OF_MONTH, Integer.parseInt("-" + daysBeforeDelete));
      deleteCutoffDate = String.format("%04d%02d%02d%02d",
          cal.get(Calendar.YEAR), (cal.get(Calendar.MONTH) + 1),
          cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY));
      LOG.info("Deleting logs from before {}", deleteCutoffDate);
    }

    long now = System.currentTimeMillis();

    // Various exceptions have been popping up here. So make sure I catch them
    // all.
    try {

      // Patterns to recognize hour, day and incoming directories, so that they
      // can be processed.
      Pattern datePathPattern;
      Pattern hourPathPattern;
      Pattern incomingPathPattern;
      Pattern dataPathPattern;
      Pattern archivePathPattern;
      Pattern workingPathPattern;
      if (hour != null) {
        datePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")");
        hourPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")/("
            + Pattern.quote(hour) + ")");
        incomingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")/("
            + Pattern.quote(hour) + ")/([^/]+)/incoming");
        dataPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")/("
            + Pattern.quote(hour) + ")/([^/]+)/data");
        archivePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")/("
            + Pattern.quote(hour) + ")/([^/]+)/archive");
        workingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")/("
            + Pattern.quote(hour) + ")/([^/]+)/working/([^/]+)_(\\d+)");
      } else if (date != null) {
        datePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")");
        hourPathPattern = Pattern
            .compile(rootDir + "/" + Pattern.quote(dcNumber) + "/"
                + Pattern.quote(service) + "/" + Pattern.quote(logdir) + "/("
                + Pattern.quote(date) + ")/(\\d{2})");
        incomingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date)
            + ")/(\\d{2})/([^/]+)/incoming");
        dataPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date)
            + ")/(\\d{2})/([^/]+)/data");
        archivePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date)
            + ")/(\\d{2})/([^/]+)/archive");
        workingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date)
            + ")/(\\d{2})/([^/]+)/working/([^/]+)_(\\d+)");
      } else {
        datePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(\\d{8})");
        hourPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(\\d{8})/(\\d{2})");
        incomingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(\\d{8})/(\\d{2})/([^/]+)/incoming");
        dataPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(\\d{8})/(\\d{2})/([^/]+)/data");
        archivePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(\\d{8})/(\\d{2})/([^/]+)/archive");
        workingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir)
            + "/(\\d{8})/(\\d{2})/([^/]+)/working/([^/]+)_(\\d+)");
      }

      // Do a depth first search of the directory, processing anything that
      // looks
      // interesting along the way
      Deque<Path> paths = new ArrayDeque<Path>();
      Path rootPath = new Path(rootDir + "/" + dcNumber + "/" + service + "/"
          + logdir + "/");
      paths.push(rootPath);

      while (paths.size() > 0) {
        Path p = paths.pop();
        LOG.debug("{}", p.toString());

        if (!fs.exists(p)) {
          continue;
        }

        FileStatus dirStatus = fs.getFileStatus(p);
        FileStatus[] children = fs.listStatus(p);
        boolean addChildren = true;

        boolean old = dirStatus.getModificationTime() < now - WAIT_TIME;
        LOG.debug("    Was last modified {}ms ago",
            now - dirStatus.getModificationTime());

        if (!old) {
          LOG.debug("    Skipping, since it's not old enough.");

        } else if ((!rootPath.equals(p))
            && (children.length == 0 || (children.length == 1 && children[0]
                .getPath().getName().equals(READY_MARKER)))) {
          // old and no children? Delete!
          LOG.info("    Deleting empty directory {}", p.toString());
          fs.delete(p, true);

        } else {
          Matcher matcher = datePathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            LOG.debug("Checking date directory");

            // If this is already done, then skip it. So only process if it
            // doesn't exist.
            if (fs.exists(new Path(p, READY_MARKER)) == false) {
              // Check each subdirectory. If they all have ready markers, then I
              // guess we're ready.
              boolean ready = true;
              for (FileStatus c : children) {
                if (c.isDirectory()
                    && fs.exists(new Path(c.getPath(), READY_MARKER)) == false) {
                  ready = false;
                  break;
                }
              }

              if (ready) {
                fs.createNewFile(new Path(p, READY_MARKER));
              }
            }
          }

          matcher = hourPathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            LOG.debug("Checking hour directory");

            // If this is already done, then skip it. So only process if it
            // doesn't exist.
            if (fs.exists(new Path(p, READY_MARKER)) == false) {
              // Check each subdirectory. If they all have ready markers, then I
              // guess we're ready.
              boolean ready = true;
              for (FileStatus c : children) {
                if (c.isDirectory()
                    && fs.exists(new Path(c.getPath(), READY_MARKER)) == false) {
                  ready = false;
                  break;
                }
              }

              if (ready) {
                fs.createNewFile(new Path(p, READY_MARKER));
              }
            }
          }

          // Check to see if we have to run a merge
          matcher = incomingPathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            LOG.debug("Checking incoming directory");
            String matchDate = matcher.group(1);
            String matchHour = matcher.group(2);
            String matchComponent = matcher.group(3);

            String timestamp = matchDate + matchHour;

            if (doDelete && timestamp.compareTo(deleteCutoffDate) < 0) {
              LOG.info("Deleting old directory: {}", p);
              fs.delete(p, true);
              addChildren = false;
            } else if (doMerge) {

              // old, looks right, and has children? Run it!
              boolean hasMatchingChildren = false;
              boolean subdirTooYoung = false;

              for (FileStatus child : children) {
                if (!hasMatchingChildren) {
                  FileStatus[] grandchildren = fs.listStatus(child.getPath());
                  for (FileStatus gc : grandchildren) {
                    if (VALID_FILE.matcher(gc.getPath().getName()).matches()) {
                      hasMatchingChildren = true;
                      break;
                    }
                  }
                }
                if (!subdirTooYoung) {
                  if (child.getModificationTime() >= now - WAIT_TIME) {
                    subdirTooYoung = true;
                    LOG.debug("    Subdir {} is too young.", child.getPath());
                  }
                }
              }

              if (!hasMatchingChildren) {
                LOG.debug("    No files match the expected pattern ({})",
                    VALID_FILE.pattern());
              }

              if (hasMatchingChildren && !subdirTooYoung) {
                LOG.info("    Run Merge job {} :: {} {} {} {} {}",
                    new Object[] { p.toString(), dcNumber, service, matchDate,
                        matchHour, matchComponent });

                Properties jobProps = new Properties();
                jobProps.load(new FileInputStream(mergeJobPropertiesFile));

                jobProps.setProperty("jobType", "merge");
                jobProps.setProperty("rootDir", rootDir);
                jobProps.setProperty("dcNumber", dcNumber);
                jobProps.setProperty("service", service);
                jobProps.setProperty("date", matchDate);
                jobProps.setProperty("hour", matchHour);
                jobProps.setProperty("component", matchComponent);
                jobProps.setProperty("user.name", userName);
                jobProps.setProperty("logdir", logdir);

                jobRunner.submit(jobProps);

                addChildren = false;
              }
            }
          }

          // Check to see if we need to run a filter and archive
          matcher = dataPathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            String matchDate = matcher.group(1);
            String matchHour = matcher.group(2);
            String matchComponent = matcher.group(3);

            String timestamp = matchDate + matchHour;

            if (doDelete && timestamp.compareTo(deleteCutoffDate) < 0) {
              LOG.info("Deleting old directory: {}", p);
              fs.delete(p, true);
              addChildren = false;
            } else if (doArchive && timestamp.compareTo(filterCutoffDate) < 0) {

              Properties jobProps = new Properties();
              jobProps.load(new FileInputStream(filterJobPropertiesFile));

              jobProps.setProperty("jobType", "filter");
              jobProps.setProperty("rootDir", rootDir);
              jobProps.setProperty("dcNumber", dcNumber);
              jobProps.setProperty("service", service);
              jobProps.setProperty("date", matchDate);
              jobProps.setProperty("hour", matchHour);
              jobProps.setProperty("component", matchComponent);
              jobProps.setProperty("user.name", userName);
              jobProps.setProperty("logdir", logdir);

              // Check to see if we should just keep all or delete all here.
              // The filter file should be here
              String appPath = jobProps
                  .getProperty("oozie.wf.application.path");
              appPath = appPath.replaceFirst("\\$\\{.*?\\}", "");
              Path filterFile = new Path(appPath + "/"
                  + conf.get("filter.definition.file", service + ".yaml"));
              LOG.info("Filter file is {}", filterFile);
              if (fs.exists(filterFile)) {
                List<BoomFilterMapper.Filter> filters = BoomFilterMapper
                    .loadFilters(matchComponent, fs.open(filterFile));

                if (filters == null) {
                  LOG.warn(
                      "    Got null when getting filters.  Not processing. {} :: {} {} {} {} {}",
                      new Object[] { p.toString(), dcNumber, service,
                          matchDate, matchHour, matchComponent });
                } else if (filters.size() == 0) {
                  LOG.warn(
                      "    Got no filters.  Not processing. {} :: {} {} {} {} {}",
                      new Object[] { p.toString(), dcNumber, service,
                          matchDate, matchHour, matchComponent });
                } else if (filters.size() == 1
                    && filters.get(0) instanceof BoomFilterMapper.KeepAllFilter) {
                  LOG.info("    Keeping everything. {} :: {} {} {} {} {}",
                      new Object[] { p.toString(), dcNumber, service,
                          matchDate, matchHour, matchComponent });
                  // Move files from data to archive
                  // delete it all!
                  String destination = rootDir + "/" + dcNumber + "/" + service
                      + "/" + logdir + "/" + matchDate + "/" + matchHour + "/"
                      + matchComponent + "/archive/";

                  PathInfo pathInfo = new PathInfo();
                  pathInfo.setDcNumber(dcNumber);
                  pathInfo.setService(service);
                  pathInfo.setLogdir(logdir);
                  pathInfo.setDate(matchDate);
                  pathInfo.setHour(matchHour);
                  pathInfo.setComponent(matchComponent);

                  try {
                    lockUtil.acquireWriteLock(lockUtil.getLockPath(pathInfo));
                    fs.mkdirs(new Path(destination));
                    for (FileStatus f : fs.listStatus(p)) {
                      fs.rename(f.getPath(), new Path(destination));
                    }
                  } finally {
                    lockUtil.releaseWriteLock(lockUtil.getLockPath(pathInfo));
                  }
                } else if (filters.size() == 1
                    && filters.get(0) instanceof BoomFilterMapper.DropAllFilter) {
                  LOG.info("    Dropping everything. {} :: {} {} {} {} {}",
                      new Object[] { p.toString(), dcNumber, service,
                          matchDate, matchHour, matchComponent });

                  PathInfo pathInfo = new PathInfo();
                  pathInfo.setDcNumber(dcNumber);
                  pathInfo.setService(service);
                  pathInfo.setLogdir(logdir);
                  pathInfo.setDate(matchDate);
                  pathInfo.setHour(matchHour);
                  pathInfo.setComponent(matchComponent);

                  try {
                    lockUtil.acquireWriteLock(lockUtil.getLockPath(pathInfo));
                    fs.delete(p, true);
                  } finally {
                    lockUtil.releaseWriteLock(lockUtil.getLockPath(pathInfo));
                  }

                } else {
                  LOG.info("    Run Filter/Archive job {} :: {} {} {} {} {}",
                      new Object[] { p.toString(), dcNumber, service,
                          matchDate, matchHour, matchComponent });
                  jobRunner.submit(jobProps);
                }
              } else {
                LOG.warn("Skipping filter job, since no filter file exists");
              }

              addChildren = false;
            }
          }

          matcher = archivePathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            String matchDate = matcher.group(1);
            String matchHour = matcher.group(2);

            String timestamp = matchDate + matchHour;

            if (doDelete && timestamp.compareTo(deleteCutoffDate) < 0) {
              LOG.info("Deleting old directory: {}", p);
              fs.delete(p, true);
              addChildren = false;
            }
          }

          matcher = workingPathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            LOG.info("  Matches working pattern ({})", p);
            if (resetOrphanedJobs) {
              String matchDate = matcher.group(1);
              String matchHour = matcher.group(2);
              String matchComponent = matcher.group(3);

              // Move everything from working/xxx/incoming/ to incoming/
              PathInfo lockPathInfo = new PathInfo(logdir, rootDir + "/"
                  + dcNumber + "/" + service + "/" + logdir + "/" + matchDate
                  + "/" + matchHour + "/" + matchComponent);
              lockUtil.acquireWriteLock(lockUtil.getLockPath(lockPathInfo));

              FileStatus[] fileStatuses = fs.listStatus(new Path(p.toUri()
                  .getPath() + "/incoming/"));
              if (fileStatuses != null) {
                for (FileStatus fileStatus : fileStatuses) {
                  Path toPath = new Path(fileStatus.getPath().getParent()
                      .getParent().getParent().getParent(), "incoming/"
                      + fileStatus.getPath().getName());

                  LOG.info("  Moving data from {} to {}", fileStatus.getPath(),
                      toPath);
                  LOG.info("    mkdir {}", toPath);
                  fs.mkdirs(toPath);

                  Path fromDir = new Path(p.toUri().getPath(), "incoming/"
                      + fileStatus.getPath().getName());
                  LOG.info("    moving from {}", fromDir);
                  FileStatus[] files = fs.listStatus(fromDir);
                  if (files == null || files.length == 0) {
                    LOG.info("    Nothing to move from  {}", fromDir);
                  } else {
                    for (FileStatus f : files) {
                      LOG.info("    rename {} {}", f.getPath(), new Path(
                          toPath, f.getPath().getName()));
                      fs.rename(f.getPath(), new Path(toPath, f.getPath()
                          .getName()));
                    }
                  }

                  LOG.info("    rm {}", fileStatus.getPath());
                  fs.delete(fileStatus.getPath(), true);
                }
                lockUtil.releaseWriteLock(lockUtil.getLockPath(lockPathInfo));

                fs.delete(new Path(p.toUri().getPath()), true);
              }
            }

            addChildren = false;
          }
        }

        // Add any children which are directories to the stack.
        if (addChildren) {
          for (int i = children.length - 1; i >= 0; i--) {
            FileStatus child = children[i];
            if (child.isDirectory()) {
              paths.push(child.getPath());
            }
          }
        }
      }

      // Since we may have deleted a bunch of directories, delete any unused
      // locks
      // from ZooKeeper.
      {
        LOG.info("Checking for unused locks in ZooKeeper");
        String scanPath = rootDir + "/" + dcNumber + "/" + service + "/"
            + logdir;
        if (date != null) {
          scanPath += "/" + date;
          if (hour != null) {
            scanPath += "/" + hour;
          }
        }

        List<LockInfo> lockInfo = lockUtil.scan(scanPath);

        for (LockInfo li : lockInfo) {
          // Check if the lock path still exists in HDFS. If it doesn't, then
          // delete it from ZooKeeper.
          String path = li.getPath();
          String hdfsPath = path.substring(LockUtil.ROOT.length());
          if (!fs.exists(new Path(hdfsPath))) {
            ZooKeeper zk = lockUtil.getZkClient();

            while (!path.equals(LockUtil.ROOT)) {
              try {
                zk.delete(path, -1);
              } catch (KeeperException.NotEmptyException e) {
                // That's fine. just stop trying then.
                break;
              } catch (Exception e) {
                LOG.error("Caught exception trying to delete from ZooKeeper.",
                    e);
                break;
              }
              LOG.info("Deleted from ZooKeeper: {}", path);
              path = path.substring(0, path.lastIndexOf('/'));
            }

          }
        }
      }

      // Now that we're done, wait for the Oozie Runner to stop, and print the
      // results.
      LOG.info("Waiting for Oozie jobs to complete.");
      jobRunner.shutdown();
      jobRunnerThread.join();
      LOG.info("Job Stats : Started={} Succeeded={} failed={} errors={}",
          new Object[] { jobRunner.getStarted(), jobRunner.getSucceeded(),
              jobRunner.getFailed(), jobRunner.getErrors() });

      lockUtil.close();

    } catch (Exception e) {
      LOG.error("Unexpected exception caught.", e);
      return 1;
    }

    return 0;
  }

  private String getConfOrEnv(Configuration conf, String propertyOrEnv,
      String defaultValue) {
    String value = getConfOrEnv(conf, propertyOrEnv);
    if (value == null) {
      LOG.info("Using default value {} = {}", new Object[] { propertyOrEnv,
          defaultValue });
      return defaultValue;
    } else {
      return value;
    }
  }

  private String getConfOrEnv(Configuration conf, String propertyOrEnv) {
    String property = propertyOrEnv.toLowerCase().replaceAll("_", ".");
    String env = propertyOrEnv.toUpperCase().replaceAll("\\.", "_");
    LOG.debug("Checking {}/{}", property, env);
    String result = conf.get(property, System.getenv(env));
    LOG.info("Option {}/{} = {}", new Object[] { property, env, result });
    return result;
  }

  private class JobRunner implements Runnable {
    private int maxConcurrentJobs;
    private int maxTotalJobs;
    private BlockingQueue<Properties> pendingQueue;
    private ThreadPoolExecutor executor;

    private int started = 0;
    private int succeeded = 0;
    private int failed = 0;
    private int errors = 0;

    private boolean shutdown = false;

    private String uuid = UUID.randomUUID().toString();

    private JobRunner(int maxConcurrentJobs) {
      configure(maxConcurrentJobs, Integer.MAX_VALUE);
    }

    private JobRunner(int maxConcurrentJobs, int maxTotalJobs) {
      configure(maxConcurrentJobs, maxTotalJobs);
    }

    private void configure(int maxConcurrentJobs, int maxTotalJobs) {
      this.maxConcurrentJobs = maxConcurrentJobs;
      if (this.maxConcurrentJobs < 1) {
        this.maxConcurrentJobs = Integer.MAX_VALUE;
      }

      this.maxTotalJobs = maxTotalJobs;
      if (this.maxTotalJobs < 1) {
        this.maxTotalJobs = Integer.MAX_VALUE;
      }

      pendingQueue = new LinkedBlockingQueue<Properties>();

      int corePoolSize = Math.min(10, this.maxConcurrentJobs);
      executor = new ThreadPoolExecutor(corePoolSize, this.maxConcurrentJobs,
          60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    }

    @Override
    public void run() {
      List<RunnerAndFuture> jobs = new ArrayList<RunnerAndFuture>();
      try {
        while (true) {
          // Run any new jobs that are waiting.
          while (started < maxTotalJobs) {
            Properties prop = pendingQueue.poll();
            if (prop == null) {
              break;
            }

            if (prop.getProperty("jobType").equals("merge")) {
              long backoff = 100;
              RunnerAndFuture job = null;
              while (job == null) {
                try {
                  job = runMerge(prop);
                } catch (RejectedExecutionException e) {
                  job = null;
                  Thread.sleep(backoff);
                  backoff = Math.min(10000, 2 * backoff);
                }
              }
              jobs.add(job);
            } else if (prop.getProperty("jobType").equals("filter")) {
              long backoff = 100;
              RunnerAndFuture job = null;
              while (job == null) {
                try {
                  job = runFilter(prop);
                } catch (RejectedExecutionException e) {
                  job = null;
                  Thread.sleep(backoff);
                  backoff = Math.min(10000, 2 * backoff);
                }
              }
              jobs.add(job);
            } else {
              LOG.error("JobType of '{}' is not recognizd.",
                  prop.getProperty("jobType"));
            }

            // Clear any finished futures
            Iterator<RunnerAndFuture> i = jobs.iterator();
            while (i.hasNext()) {
              RunnerAndFuture f = i.next();
              LOG.debug("Checking status of job {}", f.getRunner().getId());
              if (f.getFuture().isDone()) {
                f.getFuture().get();
                switch (f.getRunner().getStatus()) {
                case NOT_STARTED:
                case RUNNING:
                case ERROR:
                  LOG.error("[{}] Job is finished, but status is {}.", f
                      .getRunner().getId(), f.getRunner().getStatus());
                  errors++;
                  break;
                case SUCCESS:
                  LOG.debug("[{}] Job succeeded.", f.getRunner().getId());
                  succeeded++;
                  break;
                case FAILURE:
                  LOG.error("[{}] Job failed.", f.getRunner().getId());
                  failed++;
                  break;
                default:
                  LOG.error("[{}] Job is finished, but status is {}.", f
                      .getRunner().getId(), f.getRunner().getStatus());
                  errors++;
                  break;
                }

                i.remove();
              }
            }
          }

          // If we've started enough jobs, then throw away the rest.
          if (started >= maxTotalJobs) {
            pendingQueue.clear();
          }

          // Exit if we are done.
          if (pendingQueue.isEmpty() && shutdown == true) {
            LOG.info("Shutting down job runner.");
            for (RunnerAndFuture f : jobs) {
              LOG.debug("Waiting on job {}", f.getRunner().getId());
              f.getFuture().get();

              switch (f.getRunner().getStatus()) {
              case NOT_STARTED:
              case RUNNING:
              case ERROR:
                LOG.error("[{}] Job is finished, but status is {}.", f
                    .getRunner().getId(), f.getRunner().getStatus());
                errors++;
                break;
              case SUCCESS:
                LOG.debug("[{}] Job succeeded.", f.getRunner().getId());
                succeeded++;
                break;
              case FAILURE:
                LOG.error("[{}] Job failed.", f.getRunner().getId());
                failed++;
                break;
              default:
                LOG.error("[{}] Job is finished, but status is {}.", f
                    .getRunner().getId(), f.getRunner().getStatus());
                errors++;
                break;
              }
            }
            break;
          }

          LOG.debug("Active tasks={}, Pool Size={}, Max Pool Size={}",
              new Object[] { executor.getActiveCount(), executor.getPoolSize(),
                  executor.getMaximumPoolSize() });
          Thread.sleep(10000);
        }

      } catch (Throwable t) {
        LOG.error("Unexpected error.  Shutting down.", t);
        System.exit(1);
      }
    }

    public void submit(Properties prop) throws InterruptedException {
      pendingQueue.put(prop);
    }

    public void shutdown() {
      shutdown = true;
    }

    public int getStarted() {
      return started;
    }

    public int getSucceeded() {
      return succeeded;
    }

    public int getFailed() {
      return failed;
    }

    public int getErrors() {
      return errors;
    }

    private RunnerAndFuture runMerge(Properties prop) {
      MergeRunner runner = new MergeRunner(prop);
      Future<?> future = executor.submit(runner);
      started++;
      return new RunnerAndFuture(runner, future);
    }

    private RunnerAndFuture runFilter(Properties prop) {
      FilterRunner runner = new FilterRunner(prop);
      Future<?> future = executor.submit(runner);
      started++;
      return new RunnerAndFuture(runner, future);
    }

    private class RunnerAndFuture {
      private RunnableWithStatus runner;
      private Future<?> future;

      public RunnerAndFuture(RunnableWithStatus runner, Future<?> future) {
        this.setRunner(runner);
        this.setFuture(future);
      }

      public RunnableWithStatus getRunner() {
        return runner;
      }

      public void setRunner(RunnableWithStatus runner) {
        this.runner = runner;
      }

      public Future<?> getFuture() {
        return future;
      }

      public void setFuture(Future<?> future) {
        this.future = future;
      }
    }

    private abstract class RunnableWithStatus implements Runnable {
      public abstract Status getStatus();

      public abstract String getId();
    }

    private class MergeRunner extends RunnableWithStatus {
      @SuppressWarnings("unused")
	private String nameNode;
      private String queueName;
      private String umaskProperty;
      private String logdir;
      private String dcNumber;
      private String service;
      private String date;
      private String hour;
      private String component;
      private String baseDir;
      private String targetFileSize;
      private String blockSize;
      private String prefix;
      private String id;

      private Status status = Status.NOT_STARTED;

      public MergeRunner(Properties prop) {
        nameNode = prop.getProperty("nameNode");
        queueName = prop.getProperty("queueName");
        umaskProperty = prop.getProperty("umaskProperty");
        logdir = prop.getProperty("logdir");
        dcNumber = prop.getProperty("dcNumber");
        service = prop.getProperty("service");
        date = prop.getProperty("date");
        hour = prop.getProperty("hour");
        component = prop.getProperty("component");
        // baseDir = prop.getProperty("baseDir");
        targetFileSize = prop.getProperty("targetFileSize");
        blockSize = prop.getProperty("blockSize");
        // prefix = prop.getProperty("prefix");

        baseDir = StringUtils.join(new String[] { "/service", dcNumber,
            service, logdir, date, hour, component }, "/");
        prefix = component;

        id = startTime + "_" + uuid + "_" + started;
      }

      public Status getStatus() {
        return status;
      }

      public String getId() {
        return id;
      }

      @Override
      public void run() {
        LOG.info("[{}] Starting merge", id);
        status = Status.RUNNING;

        String lockPath;
        PathInfo pathInfo;
        try {
          pathInfo = new PathInfo();
          pathInfo.setDcNumber(dcNumber);
          pathInfo.setService(service);
          pathInfo.setLogdir(logdir);
          pathInfo.setDate(date);
          pathInfo.setHour(hour);
          pathInfo.setComponent(component);

          lockPath = lockUtil.getLockPath(pathInfo);
        } catch (Exception e) {
          LOG.error("[{}] Error creating lock path.", id, e);
          status = Status.ERROR;
          return;
        }
        LOG.info("[{}] Running merge for {}", id, pathInfo.getFullPath());

        // move to working
        try {
          LOG.info("[{}] Moving to working.", id);
          lockUtil.acquireWriteLock(lockPath);

          Path from = new Path(baseDir + "/incoming/*");
          Path to = new Path(baseDir + "/working/" + id + "/incoming/");
          fs.mkdirs(to);

          for (FileStatus filestatus : fs.globStatus(from)) {
            fs.rename(filestatus.getPath(), to);
          }

        } catch (Exception e) {
          LOG.error("[{}] Error moving to working.", id, e);
          status = Status.ERROR;
          return;
        } finally {
          try {
            lockUtil.releaseWriteLock(lockPath);
          } catch (Exception e) {
            LOG.error("[{}] Error releasing write lock.", id, e);
            status = Status.ERROR;
            return;
          }
        }

        // run merge
        int mapredStatus;
        Job job;
        try {
          job = new Job(getConf());
        } catch (IOException e) {
          LOG.error("[{}] Error setting up job.", id, e);
          return;
        }
        Configuration jobConf = job.getConfiguration();
        job.setJarByClass(AvroBlockInputFormat.class);
        job.setJobName("Merge Job : " + pathInfo.getFullPath());

        jobConf.set("mapreduce.jobtracker.split.metainfo.maxsize", "100000000");
        jobConf.set(umaskProperty, "027");
        jobConf.set("mapred.job.queue.name", queueName);
        jobConf.set("mapred.max.split.size", targetFileSize);
        jobConf.set("dfs.block.size", blockSize);
        jobConf.set("mapred.input.format.class",
            "com.blackberry.logdriver.mapred.avro.AvroBlockInputFormat");
        jobConf.set("mapred.mapper.class",
            "com.blackberry.logdriver.mapred.avro.AvroBlockWriterMapper");
        jobConf.set("mapred.output.key.class",
            "org.apache.hadoop.io.BytesWritable");
        jobConf.set("mapred.output.value.class",
            "org.apache.hadoop.io.NullWritable");
        jobConf.set("mapred.output.format.class",
            "com.blackberry.logdriver.mapred.BinaryOutputFormat");
        jobConf.set("output.file.extension", ".bm");
        jobConf.set("mapred.reduce.tasks", "0");
        jobConf.set("mapred.input.dir", baseDir + "/working/" + id
            + "/incoming/*");
        jobConf.set("mapred.output.dir", baseDir + "/working/" + id + "/data");
        jobConf.set("logdriver.output.file.prefix", prefix);

        try {
          mapredStatus = job.waitForCompletion(true) ? 0 : 1;
        } catch (IOException e) {
          LOG.error("Error running job.", e);
          status = Status.ERROR;
          return;
        } catch (InterruptedException e) {
          LOG.error("Job interrupted.", e);
          status = Status.ERROR;
          return;
        } catch (ClassNotFoundException e) {
          LOG.error("Error running job.", e);
          status = Status.ERROR;
          return;
        }

        if (mapredStatus == 1) {
          LOG.error("Error running job.");

          // move to failed
          try {
            LOG.info("[{}] Moving to /failed.", id);
            lockUtil.acquireWriteLock(lockPath);

            Path from = new Path(baseDir + "/working/" + id);
            Path to = new Path(baseDir + "/failed/");

            fs.mkdirs(to);
            fs.rename(from, to);
          } catch (Exception e) {
            LOG.error("[{}] Error moving to /failed.", id, e);
            status = Status.ERROR;
            return;
          } finally {
            try {
              lockUtil.releaseWriteLock(lockPath);
            } catch (Exception e) {
              LOG.error("[{}] Error releasing write lock.", id, e);
              status = Status.ERROR;
              return;
            }
          }

          status = Status.FAILURE;
        } else {
          // move to data and delete.
          try {
            LOG.info("[{}] Moving to /data.", id);
            lockUtil.acquireWriteLock(lockPath);

            Path from = new Path(baseDir + "/working/" + id + "/data/*.bm");
            Path to = new Path(baseDir + "/data/");
            // Create destination dir
            fs.mkdirs(to);

            // Move the boom files
            for (FileStatus filestatus : fs.globStatus(from)) {
              fs.rename(filestatus.getPath(), to);
            }

            // Touch the ready files
            fs.create(new Path(baseDir + "/data/_READY")).close();
            fs.create(new Path(baseDir + "/_READY")).close();
            // Delete the working dir
            fs.delete(new Path(baseDir + "/working/" + id), true);

          } catch (Exception e) {
            LOG.error("[{}] Error moving to /data.", id, e);
            status = Status.ERROR;
            return;
          } finally {
            try {
              lockUtil.releaseWriteLock(lockPath);
            } catch (Exception e) {
              LOG.error("[{}] Error releasing write lock.", id, e);
              status = Status.ERROR;
              return;
            }
          }

          status = Status.SUCCESS;
        }
      }
    }

    private class FilterRunner extends RunnableWithStatus {
      @SuppressWarnings("unused")
	private String nameNode;
      private String queueName;
      private String umaskProperty;
      private String logdir;
      private String dcNumber;
      private String service;
      private String date;
      private String hour;
      private String component;
      private String baseDir;
      private String targetFileSize;
      private String blockSize;
      private String prefix;
      private String id;

      private Status status = Status.NOT_STARTED;

      public FilterRunner(Properties prop) {
        nameNode = prop.getProperty("nameNode");
        queueName = prop.getProperty("queueName");
        umaskProperty = prop.getProperty("umaskProperty");
        logdir = prop.getProperty("logdir");
        dcNumber = prop.getProperty("dcNumber");
        service = prop.getProperty("service");
        date = prop.getProperty("date");
        hour = prop.getProperty("hour");
        component = prop.getProperty("component");
        // baseDir = prop.getProperty("baseDir");
        targetFileSize = prop.getProperty("targetFileSize");
        blockSize = prop.getProperty("blockSize");
        // prefix = prop.getProperty("prefix");

        baseDir = StringUtils.join(new String[] { "/service", dcNumber,
            service, logdir, date, hour, component }, "/");
        prefix = component;

        id = startTime + "_" + uuid + "_" + started;
      }

      public Status getStatus() {
        return status;
      }

      public String getId() {
        return id;
      }

      @Override
      public void run() {
        LOG.info("[{}] Starting filter", id);
        status = Status.RUNNING;

        String lockPath;
        PathInfo pathInfo;
        try {
          pathInfo = new PathInfo();
          pathInfo.setDcNumber(dcNumber);
          pathInfo.setService(service);
          pathInfo.setLogdir(logdir);
          pathInfo.setDate(date);
          pathInfo.setHour(hour);
          pathInfo.setComponent(component);

          lockPath = lockUtil.getLockPath(pathInfo);
        } catch (Exception e) {
          LOG.error("[{}] Error creating lock path.", id, e);
          status = Status.ERROR;
          return;
        }
        LOG.info("[{}] Running filter for {}", id, pathInfo.getFullPath());

        // move to working
        try {
          LOG.info("[{}] Moving to working.", id);
          lockUtil.acquireWriteLock(lockPath);

          // move ${nameNode}${baseDir}/data/*
          // ${baseDir}/working/${wf:id()}_${wf:run()}/incoming/
          Path from = new Path(baseDir + "/data/*");
          Path to = new Path(baseDir + "/working/" + id + "/incoming/");
          fs.mkdirs(to);

          for (FileStatus filestatus : fs.globStatus(from)) {
            fs.rename(filestatus.getPath(), to);
          }

        } catch (Exception e) {
          LOG.error("[{}] Error moving to working.", id, e);
          status = Status.ERROR;
          return;
        } finally {
          try {
            lockUtil.releaseWriteLock(lockPath);
          } catch (Exception e) {
            LOG.error("[{}] Error releasing write lock.", id, e);
            status = Status.ERROR;
            return;
          }
        }

        // run filter
        int mapredStatus;
        {
          Job job;
          try {
            job = new Job(getConf());
          } catch (IOException e) {
            LOG.error("[{}] Error setting up job.", id, e);
            status = Status.ERROR;
            return;
          }
          Configuration jobConf = job.getConfiguration();
          job.setJarByClass(AvroBlockInputFormat.class);
          job.setJobName("Filter Job : " + pathInfo.getFullPath());

          jobConf.set("mapreduce.jobtracker.split.metainfo.maxsize",
              "100000000");
          jobConf.set(umaskProperty, "027");
          jobConf.set("mapred.job.queue.name", queueName);
          jobConf.set("mapred.input.format.class",
              "com.blackberry.logdriver.mapred.boom.BoomInputFormat");
          jobConf.set("mapred.mapper.class",
              "com.blackberry.logdriver.mapred.boom.BoomFilterMapper");
          jobConf.set("mapred.output.key.class",
              "com.blackberry.logdriver.boom.LogLineData");
          jobConf.set("mapred.output.value.class", "org.apache.hadoop.io.Text");
          jobConf.set("mapred.output.format.class",
              "com.blackberry.logdriver.mapred.boom.ReBoomOutputFormat");
          jobConf.set("mapred.reduce.tasks", "0");
          jobConf.set("mapred.input.dir", baseDir + "/working/" + id
              + "/incoming/*.bm");
          jobConf.set("mapred.output.dir", baseDir + "/working/" + id
              + "/filtered");
          jobConf.set("logdriver.filter.file", "configuration.yaml");
          jobConf.set("logdriver.component.name", component);

          try {
            Path path = new Path(FILTER_LOCATION + service
                + ".yaml#configuration.yaml");
            DistributedCache.addCacheFile(new URI(path.toUri().getPath()),
                jobConf);
            DistributedCache.createSymlink(jobConf);
          } catch (URISyntaxException e) {
            LOG.error("Error adding file to distributed cache", e);
            status = Status.ERROR;
            return;
          }

          try {
            LOG.info("[{}] Running filter job", id);
            mapredStatus = job.waitForCompletion(true) ? 0 : 1;
          } catch (IOException e) {
            LOG.error("[{}] Error running job.", id, e);
            status = Status.ERROR;
            return;
          } catch (InterruptedException e) {
            LOG.error("[{}] Job interrupted.", id, e);
            status = Status.ERROR;
            return;
          } catch (ClassNotFoundException e) {
            LOG.error("[{}] Error running job.", id, e);
            status = Status.ERROR;
            return;
          }
        }
        if (mapredStatus == 1) {
          LOG.error("Error running job.");

          // move to failed
          try {
            LOG.info("[{}] Moving to /failed.", id);
            lockUtil.acquireWriteLock(lockPath);

            Path from = new Path(baseDir + "/working/" + id);
            Path to = new Path(baseDir + "/failed/");

            fs.mkdirs(to);
            fs.rename(from, to);
          } catch (Exception e) {
            LOG.error("[{}] Error moving to /failed.", id, e);
            status = Status.ERROR;
            return;
          } finally {
            try {
              lockUtil.releaseWriteLock(lockPath);
            } catch (Exception e) {
              LOG.error("[{}] Error releasing write lock.", id, e);
              status = Status.ERROR;
              return;
            }
          }

          status = Status.FAILURE;
          return;
        }

        // run merge
        {
          Job job;
          try {
            job = new Job(getConf());
          } catch (IOException e) {
            LOG.error("Error setting up job.", e);
            return;
          }
          Configuration jobConf = job.getConfiguration();
          job.setJarByClass(AvroBlockInputFormat.class);
          job.setJobName("Merge Job : " + pathInfo.getFullPath());

          jobConf.set("mapreduce.jobtracker.split.metainfo.maxsize",
              "100000000");
          jobConf.set(umaskProperty, "027");
          jobConf.set("mapred.job.queue.name", queueName);
          jobConf.set("mapred.max.split.size", targetFileSize);
          jobConf.set("dfs.block.size", blockSize);
          jobConf.set("mapred.input.format.class",
              "com.blackberry.logdriver.mapred.avro.AvroBlockInputFormat");
          jobConf.set("mapred.mapper.class",
              "com.blackberry.logdriver.mapred.avro.AvroBlockWriterMapper");
          jobConf.set("mapred.output.key.class",
              "org.apache.hadoop.io.BytesWritable");
          jobConf.set("mapred.output.value.class",
              "org.apache.hadoop.io.NullWritable");
          jobConf.set("mapred.output.format.class",
              "com.blackberry.logdriver.mapred.BinaryOutputFormat");
          jobConf.set("output.file.extension", ".bm");
          jobConf.set("mapred.reduce.tasks", "0");
          jobConf.set("mapred.input.dir", baseDir + "/working/" + id
              + "/filtered/*.bm");
          jobConf.set("mapred.output.dir", baseDir + "/working/" + id
              + "/archive");
          jobConf.set("logdriver.output.file.prefix", prefix);

          try {
            mapredStatus = job.waitForCompletion(true) ? 0 : 1;
          } catch (InterruptedException e) {
            LOG.error("Job interrupted.", e);
            status = Status.ERROR;
            return;
          } catch (Throwable e) {
            LOG.error("Error running job.", e);
            status = Status.ERROR;
            return;
          }
        }
        if (mapredStatus == 1) {
          LOG.error("Error running job.");

          // move to failed
          try {
            LOG.info("[{}] Moving to /failed.", id);
            lockUtil.acquireWriteLock(lockPath);

            Path from = new Path(baseDir + "/working/" + id);
            Path to = new Path(baseDir + "/failed/");

            fs.mkdirs(to);
            fs.rename(from, to);
          } catch (Exception e) {
            LOG.error("[{}] Error moving to /failed.", id, e);
            status = Status.ERROR;
            return;
          } finally {
            try {
              lockUtil.releaseWriteLock(lockPath);
            } catch (Exception e) {
              LOG.error("[{}] Error releasing write lock.", id, e);
              status = Status.ERROR;
              return;
            }
          }

          status = Status.FAILURE;
          return;
        }

        // move to archive and delete.
        try {
          LOG.info("[{}] Moving to /archive.", id);
          lockUtil.acquireWriteLock(lockPath);

          // move
          // ${nameNode}${baseDir}/working/${wf:id()}_${wf:run()}/archive/*.bm
          // ${baseDir}/archive/</arg>
          // delete ${nameNode}${baseDir}/working/${wf:id()}_${wf:run()}</arg>
          Path from = new Path(baseDir + "/working/" + id + "/archive/*.bm");
          Path to = new Path(baseDir + "/archive/");
          // Create destination dir
          fs.mkdirs(to);

          // Move the boom files
          for (FileStatus filestatus : fs.globStatus(from)) {
            fs.rename(filestatus.getPath(), to);
          }

          // Delete the working dir
          fs.delete(new Path(baseDir + "/working/" + id), true);

        } catch (Exception e) {
          LOG.error("[{}] Error moving to /data.", id, e);
          status = Status.ERROR;
          return;
        } finally {
          try {
            lockUtil.releaseWriteLock(lockPath);
          } catch (Exception e) {
            LOG.error("[{}] Error releasing write lock.", id, e);
            status = Status.ERROR;
            return;
          }
        }

        status = Status.SUCCESS;
      }
    }
  }

  public void printUsage() {
    System.out.println("Usage: " + this.getClass().getSimpleName()
        + " <user.name> <site number> <service> [yyyymmdd [hh]]");
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new LogMaintenance(), args);
    System.exit(res);
  }

}
