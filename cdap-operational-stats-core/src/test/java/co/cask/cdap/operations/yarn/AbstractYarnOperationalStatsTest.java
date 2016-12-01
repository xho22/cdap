/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.operations.yarn;

import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.operations.OperationalStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests {@link OperationalStats} for Yarn.
 */
public abstract class AbstractYarnOperationalStatsTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractYarnOperationalStatsTest.class);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private MiniYARNCluster yarnCluster;
  private Configuration conf;

  protected abstract MiniYARNCluster createYarnCluster() throws IOException, InterruptedException, YarnException;

  protected abstract int getNumNodes();

  @Before
  public void setup() throws Exception {
    yarnCluster = createYarnCluster();
    yarnCluster.waitForNodeManagersToConnect(5000);
    conf = yarnCluster.getResourceManager().getConfig();
  }

  @After
  public void teardown() {
    if (yarnCluster != null) {
      yarnCluster.stop();
    }
  }

  @Test
  public void test() throws Exception {
    YarnInfo info = new YarnInfo(conf);
    Assert.assertEquals("YARN", info.getServiceName());
    Assert.assertEquals("info", info.getStatType());
    Assert.assertNotNull(info.getVersion());
    Assert.assertNull(info.getWebURL());
    Assert.assertNull(info.getLogsURL());
    info.collect();
    Assert.assertNotNull(info.getWebURL());
    Assert.assertNotNull(info.getLogsURL());
    Assert.assertEquals(info.getWebURL() + "/logs", info.getLogsURL());
    YarnApps apps = new YarnApps(conf);
    Assert.assertEquals("YARN", apps.getServiceName());
    Assert.assertEquals("apps", apps.getStatType());
    apps.collect();
    Assert.assertEquals(0, apps.getAccepted());
    Assert.assertEquals(0, apps.getFailed());
    Assert.assertEquals(0, apps.getFinished());
    Assert.assertEquals(0, apps.getKilled());
    Assert.assertEquals(0, apps.getNew());
    Assert.assertEquals(0, apps.getRunning());
    Assert.assertEquals(0, apps.getSubmitted());
    Assert.assertEquals(0, apps.getTotal());
    final YarnResources resources = new YarnResources(conf);
    Assert.assertEquals("YARN", resources.getServiceName());
    Assert.assertEquals("resources", resources.getStatType());
    // wait until node manager reports are available
    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          resources.collect();
          return resources.getTotalMemory() > 0;
        }
      }, 10, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      NodeManager nodeManager = yarnCluster.getNodeManager(0);
      NodeHealthStatus nodeHealthStatus = nodeManager.getNMContext().getNodeHealthStatus();
      LOG.error("nodemanager health status = {}", nodeHealthStatus);
      nodeManager.getNodeStatusUpdater().sendOutofBandHeartBeat();
      resources.collect();
      LOG.error("resources total memory = {}", resources.getTotalMemory());
      LOG.error("disks healthy = {}", nodeManager.getNodeHealthChecker().getDiskHandler().areDisksHealthy());
      nodeManager.getNodeStatusUpdater().getNodeStatusAndUpdateContainersInContext(0);
      resources.collect();
      LOG.error("resources total memory = {}", resources.getTotalMemory());
    }
    Assert.assertEquals(0, resources.getUsedMemory());
    Assert.assertEquals(resources.getTotalMemory(), resources.getFreeMemory());
    Assert.assertTrue(resources.getTotalVCores() > 0);
    Assert.assertEquals(0, resources.getUsedVCores());
    Assert.assertEquals(resources.getTotalVCores(), resources.getFreeVCores());
    YarnQueues queues = new YarnQueues(conf);
    Assert.assertEquals("YARN", queues.getServiceName());
    Assert.assertEquals("queues", queues.getStatType());
    Assert.assertEquals(0, queues.getStopped());
    Assert.assertEquals(0, queues.getStopped());
    Assert.assertEquals(0, queues.getStopped());
    queues.collect();
    Assert.assertTrue(queues.getRunning() > 0);
    Assert.assertEquals(0, queues.getStopped());
    Assert.assertEquals(queues.getRunning(), queues.getTotal());
    YarnNodes nodes = new YarnNodes(conf);
    Assert.assertEquals("YARN", nodes.getServiceName());
    Assert.assertEquals("nodes", nodes.getStatType());
    Assert.assertEquals(0, nodes.getTotalNodes());
    Assert.assertEquals(0, nodes.getHealthyNodes());
    Assert.assertEquals(0, nodes.getNewNodes());
    Assert.assertEquals(0, nodes.getUnusableNodes());
    Assert.assertEquals(0, nodes.getTotalContainers());
    Assert.assertEquals(0, nodes.getHealthyContainers());
    Assert.assertEquals(0, nodes.getNewContainers());
    Assert.assertEquals(0, nodes.getUnusableContainers());
    nodes.collect();
    Assert.assertEquals(getNumNodes(), nodes.getTotalNodes());
    Assert.assertEquals(getNumNodes(), nodes.getHealthyNodes());
    Assert.assertEquals(0, nodes.getNewNodes());
    Assert.assertEquals(0, nodes.getUnusableNodes());
    Assert.assertEquals(0, nodes.getTotalContainers());
    Assert.assertEquals(0, nodes.getHealthyContainers());
    Assert.assertEquals(0, nodes.getNewContainers());
    Assert.assertEquals(0, nodes.getUnusableContainers());
  }
}
