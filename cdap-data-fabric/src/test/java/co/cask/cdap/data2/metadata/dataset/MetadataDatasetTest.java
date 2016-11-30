/*
 * Copyright 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.metadata.indexer.Indexer;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test class for {@link MetadataDataset} class.
 */
public class MetadataDatasetTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final DatasetId datasetInstance = DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("meta");

  private MetadataDataset dataset;
  private TransactionExecutor txnl;

  private final ApplicationId app1 = new ApplicationId("ns1", "app1");
  private final ApplicationId appNs2 = new ApplicationId("ns2", "app1");
  // Have to use Id.Program for comparison here because the MetadataDataset APIs return Id.Program.
  private final ProgramId flow1 = new ProgramId("ns1", "app1", ProgramType.FLOW, "flow1");
  private final DatasetId dataset1 = new DatasetId("ns1", "ds1");
  private final StreamId stream1 = new StreamId("ns1", "s1");
  private final StreamViewId view1 = new StreamViewId(stream1.getNamespace(), stream1.getStream(), "v1");
  private final co.cask.cdap.proto.id.ArtifactId artifact1 =
    new co.cask.cdap.proto.id.ArtifactId("ns1", "a1", "1.0.0");

  @Before
  public void before() throws Exception {
    dataset = getDataset(datasetInstance);
    txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);
  }

  @After
  public void after() throws Exception {
    dataset = null;
    DatasetAdmin admin = dsFrameworkUtil.getFramework().getAdmin(datasetInstance, null);
    Assert.assertNotNull(admin);
    admin.truncate();
  }

  @Test
  public void testProperties() throws Exception {
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(0, dataset.getProperties(app1).size());
        Assert.assertEquals(0, dataset.getProperties(flow1).size());
        Assert.assertEquals(0, dataset.getProperties(dataset1).size());
        Assert.assertEquals(0, dataset.getProperties(stream1).size());
        Assert.assertEquals(0, dataset.getProperties(view1).size());
        Assert.assertEquals(0, dataset.getProperties(artifact1).size());
      }
    });
    // Set some properties
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(app1, "akey1", "avalue1");
        dataset.setProperty(flow1, "fkey1", "fvalue1");
        dataset.setProperty(flow1, "fK", "fV");
        dataset.setProperty(dataset1, "dkey1", "dvalue1");
        dataset.setProperty(stream1, "skey1", "svalue1");
        dataset.setProperty(stream1, "skey2", "svalue2");
        dataset.setProperty(view1, "vkey1", "vvalue1");
        dataset.setProperty(view1, "vkey2", "vvalue2");
        dataset.setProperty(artifact1, "rkey1", "rvalue1");
        dataset.setProperty(artifact1, "rkey2", "rvalue2");
      }
    });
    // verify
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Map<String, String> properties = dataset.getProperties(app1);
        Assert.assertEquals(ImmutableMap.of("akey1", "avalue1"), properties);
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.removeProperties(app1, "akey1");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertNull(dataset.getProperty(app1, "akey1"));
        MetadataEntry result = dataset.getProperty(flow1, "fkey1");
        MetadataEntry expected = new MetadataEntry(flow1, "fkey1", "fvalue1");
        Assert.assertEquals(expected, result);
        Assert.assertEquals(ImmutableMap.of("fkey1", "fvalue1", "fK", "fV"), dataset.getProperties(flow1));
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.removeProperties(flow1, "fkey1");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Map<String, String> properties = dataset.getProperties(flow1);
        Assert.assertEquals(1, properties.size());
        Assert.assertEquals("fV", properties.get("fK"));
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.removeProperties(flow1);
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(0, dataset.getProperties(flow1).size());
        MetadataEntry expected = new MetadataEntry(dataset1, "dkey1", "dvalue1");
        Assert.assertEquals(expected, dataset.getProperty(dataset1, "dkey1"));
        Assert.assertEquals(ImmutableMap.of("skey1", "svalue1", "skey2", "svalue2"), dataset.getProperties(stream1));
        Map<String, String> properties = dataset.getProperties(artifact1);
        Assert.assertEquals(ImmutableMap.of("rkey1", "rvalue1", "rkey2", "rvalue2"), properties);
        MetadataEntry result = dataset.getProperty(artifact1, "rkey2");
        expected = new MetadataEntry(artifact1, "rkey2", "rvalue2");
        Assert.assertEquals(expected, result);
        properties = dataset.getProperties(view1);
        Assert.assertEquals(ImmutableMap.of("vkey1", "vvalue1", "vkey2", "vvalue2"), properties);
        result = dataset.getProperty(view1, "vkey2");
        expected = new MetadataEntry(view1, "vkey2", "vvalue2");
        Assert.assertEquals(expected, result);
      }
    });
    // reset a property
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(stream1, "skey1", "sv1");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(ImmutableMap.of("skey1", "sv1", "skey2", "svalue2"), dataset.getProperties(stream1));
      }
    });
    // cleanup
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.removeProperties(app1);
        dataset.removeProperties(flow1);
        dataset.removeProperties(dataset1);
        dataset.removeProperties(stream1);
        dataset.removeProperties(artifact1);
        dataset.removeProperties(view1);
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(0, dataset.getProperties(app1).size());
        Assert.assertEquals(0, dataset.getProperties(flow1).size());
        Assert.assertEquals(0, dataset.getProperties(dataset1).size());
        Assert.assertEquals(0, dataset.getProperties(stream1).size());
        Assert.assertEquals(0, dataset.getProperties(view1).size());
        Assert.assertEquals(0, dataset.getProperties(artifact1).size());
      }
    });
  }

  @Test
  public void testTags() throws InterruptedException, TransactionFailureException {
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(0, dataset.getTags(app1).size());
        Assert.assertEquals(0, dataset.getTags(flow1).size());
        Assert.assertEquals(0, dataset.getTags(dataset1).size());
        Assert.assertEquals(0, dataset.getTags(stream1).size());
        Assert.assertEquals(0, dataset.getTags(view1).size());
        Assert.assertEquals(0, dataset.getTags(artifact1).size());
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.addTags(app1, "tag1", "tag2", "tag3");
        dataset.addTags(flow1, "tag1");
        dataset.addTags(dataset1, "tag3", "tag2");
        dataset.addTags(stream1, "tag2");
        dataset.addTags(view1, "tag4");
        dataset.addTags(artifact1, "tag3");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Set<String> tags = dataset.getTags(app1);
        Assert.assertEquals(3, tags.size());
        Assert.assertTrue(tags.contains("tag1"));
        Assert.assertTrue(tags.contains("tag2"));
        Assert.assertTrue(tags.contains("tag3"));
      }
    });
    // add the same tag again
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.addTags(app1, "tag1");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(3, dataset.getTags(app1).size());
        Set<String> tags = dataset.getTags(flow1);
        Assert.assertEquals(1, tags.size());
        Assert.assertTrue(tags.contains("tag1"));
        tags = dataset.getTags(dataset1);
        Assert.assertEquals(2, tags.size());
        Assert.assertTrue(tags.contains("tag3"));
        Assert.assertTrue(tags.contains("tag2"));
        tags = dataset.getTags(stream1);
        Assert.assertEquals(1, tags.size());
        Assert.assertTrue(tags.contains("tag2"));
        tags = dataset.getTags(view1);
        Assert.assertEquals(1, tags.size());
        Assert.assertTrue(tags.contains("tag4"));
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.removeTags(app1, "tag1", "tag2");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Set<String> tags = dataset.getTags(app1);
        Assert.assertEquals(1, tags.size());
        Assert.assertTrue(tags.contains("tag3"));
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.removeTags(dataset1, "tag3");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Set<String> tags = dataset.getTags(dataset1);
        Assert.assertEquals(1, tags.size());
        Assert.assertTrue(tags.contains("tag2"));
        tags = dataset.getTags(artifact1);
        Assert.assertEquals(1, tags.size());
        Assert.assertTrue(tags.contains("tag3"));
      }
    });
    // cleanup
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.removeTags(app1);
        dataset.removeTags(flow1);
        dataset.removeTags(dataset1);
        dataset.removeTags(stream1);
        dataset.removeTags(view1);
        dataset.removeTags(artifact1);
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(0, dataset.getTags(app1).size());
        Assert.assertEquals(0, dataset.getTags(flow1).size());
        Assert.assertEquals(0, dataset.getTags(dataset1).size());
        Assert.assertEquals(0, dataset.getTags(stream1).size());
        Assert.assertEquals(0, dataset.getTags(view1).size());
        Assert.assertEquals(0, dataset.getTags(artifact1).size());
      }
    });
  }

  @Test
  public void testSearchOnTags() throws Exception {
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(0, dataset.getTags(app1).size());
        Assert.assertEquals(0, dataset.getTags(appNs2).size());
        Assert.assertEquals(0, dataset.getTags(flow1).size());
        Assert.assertEquals(0, dataset.getTags(dataset1).size());
        Assert.assertEquals(0, dataset.getTags(stream1).size());
        dataset.addTags(app1, "tag1", "tag2", "tag3");
        dataset.addTags(appNs2, "tag1", "tag2", "tag3_more");
        dataset.addTags(flow1, "tag1");
        dataset.addTags(dataset1, "tag3", "tag2", "tag12-tag33");
        dataset.addTags(stream1, "tag2, tag4");
      }
    });

    // Try to search on all tags
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> results =
          dataset.search("ns1", "tags:*", ImmutableSet.of(MetadataSearchTargetType.ALL));
        // results for dataset1 - ns1:tags:tag12, ns1:tags:tag2, ns1:tags:tag3, ns1:tags:tag33, ns1:tags:tag12-tag33
        Assert.assertEquals(11, results.size());

        // Try to search for tag1*
        results = dataset.search("ns1", "tags:tag1*", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(4, results.size());

        // Try to search for tag1 with spaces in search query and mixed case of tags keyword
        results = dataset.search("ns1", "  tAGS  :  tag1  ", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(2, results.size());

        // Try to search for tag4
        results = dataset.search("ns1", "tags:tag4", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(1, results.size());

        // Try to search for tag33
        results = dataset.search("ns1", "tags:tag33", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(1, results.size());

        // Try to search for a tag which has - in it
        results = dataset.search("ns1", "tag12-tag33", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(1, results.size());

        // Try to search for tag33 with spaces in query
        results = dataset.search("ns1", "  tag33  ", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(1, results.size());

        // Try to search for tag3
        results = dataset.search("ns1", "tags:tag3*", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(3, results.size());

        // try search in another namespace
        results = dataset.search("ns2", "tags:tag1", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(1, results.size());

        results = dataset.search("ns2", "tag3", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(1, results.size());

        results = dataset.search("ns2", "tag*", ImmutableSet.of(MetadataSearchTargetType.APP));
        // 9 due to matches of type ns2:tag1, ns2:tags:tag1, and splitting of tag3_more
        Assert.assertEquals(9, results.size());

      }
    });
    // cleanup
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.removeTags(app1);
        dataset.removeTags(flow1);
        dataset.removeTags(dataset1);
        dataset.removeTags(stream1);
      }
    });
    // Search should be empty after deleting tags
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> results =
          dataset.search("ns1", "tags:tag3*", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(0, results.size());
        Assert.assertEquals(0, dataset.getTags(app1).size());
        Assert.assertEquals(0, dataset.getTags(flow1).size());
        Assert.assertEquals(0, dataset.getTags(dataset1).size());
        Assert.assertEquals(0, dataset.getTags(stream1).size());
      }
    });
  }

  @Test
  public void testSearchOnValue() throws Exception {
    // Add some metadata
    final MetadataEntry entry = new MetadataEntry(flow1, "key1", "value1");
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    final MetadataEntry multiWordEntry = new MetadataEntry(flow1, "multiword", multiWordValue);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(flow1, "key1", "value1");
        dataset.setProperty(flow1, "key2", "value2");
        dataset.setProperty(flow1, "multiword", multiWordValue);
      }
    });

    // Search for it based on value
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> results =
          dataset.search("ns1", "value1", ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        Assert.assertEquals(ImmutableList.of(entry), results);

        // Search for it based on a word in value with spaces in search query
        results = dataset.search("ns1", "  aV1   ", ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        Assert.assertEquals(ImmutableList.of(multiWordEntry), results);

        // Search for it based split patterns to make sure nothing is matched
        results = dataset.search("ns1", "-", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertTrue(results.isEmpty());
        results = dataset.search("ns1", ",", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertTrue(results.isEmpty());
        results = dataset.search("ns1", "_", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertTrue(results.isEmpty());
        results = dataset.search("ns1", ", ,", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertTrue(results.isEmpty());
        results = dataset.search("ns1", ", - ,", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertTrue(results.isEmpty());

        // Search for it based on a word in value
        results = dataset.search("ns1", "av5", ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        Assert.assertEquals(ImmutableList.of(multiWordEntry), results);

        // Case insensitive
        results = dataset.search("ns1", "ValUe1", ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        Assert.assertEquals(ImmutableList.of(entry), results);

      }
    });
    // Search based on value
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(flow1, "key3", "value1");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> results =
          dataset.search("ns1", "value1", ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        Assert.assertEquals(2, results.size());
        for (MetadataEntry result : results) {
          Assert.assertEquals("value1", result.getValue());
        }
      }
    });

    // Search based on value prefix
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(stream1, "key21", "value21");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> results =
          dataset.search("ns1", "value2*", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(2, results.size());
        for (MetadataEntry result : results) {
          Assert.assertTrue(result.getValue().startsWith("value2"));
        }
        // Search based on value prefix in the wrong namespace
        results = dataset.search("ns12", "value2*", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertTrue(results.isEmpty());
      }
    });

  }

  @Test
  public void testSearchOnKeyValue() throws Exception {
    final MetadataEntry flowEntry1 = new MetadataEntry(flow1, "key1", "value1");
    final MetadataEntry flowEntry2 = new MetadataEntry(flow1, "key2", "value2");
    final String multiWordKey = "multiword";
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    final MetadataEntry streamEntry1 = new MetadataEntry(stream1, "Key1", "Value1");
    final MetadataEntry streamEntry2 = new MetadataEntry(stream1, "sKey1", "sValue1");

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Add some properties to flow1
        dataset.setProperty(flow1, "key1", "value1");
        dataset.setProperty(flow1, "key2", "value2");
        // add a multi word value
        dataset.setProperty(flow1, multiWordKey, multiWordValue);
        dataset.setProperty(stream1, "sKey1", "sValue1");
        dataset.setProperty(stream1, "Key1", "Value1");
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Search for it based on value
        List<MetadataEntry> results =
          dataset.search("ns1", "key1" + MetadataDataset.KEYVALUE_SEPARATOR + "value1",
                         ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        Assert.assertEquals(ImmutableList.of(flowEntry1), results);

        // Search for it based on a word in value with spaces in search query
        results = dataset.search("ns1", "  multiword" + MetadataDataset.KEYVALUE_SEPARATOR + "aV1   ",
                                 ImmutableSet.of(MetadataSearchTargetType.PROGRAM));

        MetadataEntry flowMultiWordEntry = new MetadataEntry(flow1, multiWordKey, multiWordValue);
        Assert.assertEquals(ImmutableList.of(flowMultiWordEntry), results);

        // Search for it based on a word in value
        results =
          dataset.search("ns1", multiWordKey + MetadataDataset.KEYVALUE_SEPARATOR + "aV5",
                         ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        Assert.assertEquals(ImmutableList.of(flowMultiWordEntry), results);
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.removeProperties(flow1, multiWordKey);
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> results =
          dataset.search("ns1", multiWordKey + MetadataDataset.KEYVALUE_SEPARATOR + "aV5",
                         ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        // search results should be empty after removing this key as the indexes are deleted
        Assert.assertTrue(results.isEmpty());

        // Test wrong ns
        List<MetadataEntry> results2  =
          dataset.search("ns12", "key1" + MetadataDataset.KEYVALUE_SEPARATOR + "value1",
                         ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        Assert.assertTrue(results2.isEmpty());

        // Test multi word query
        results = dataset.search("ns1", "  value1  av2 ", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(Sets.newHashSet(flowEntry1, streamEntry1), Sets.newHashSet(results));

        results = dataset.search("ns1", "  value1  sValue1 ", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(Sets.newHashSet(flowEntry1, streamEntry1, streamEntry2), Sets.newHashSet(results));

        results = dataset.search("ns1", "  valu*  sVal* ", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(Sets.newHashSet(flowEntry1, flowEntry2, streamEntry1, streamEntry2),
                            Sets.newHashSet(results));

        // Using empty filter should also search for all target types
        results = dataset.search("ns1", "  valu*  sVal* ", ImmutableSet.<MetadataSearchTargetType>of());
        Assert.assertEquals(Sets.newHashSet(flowEntry1, flowEntry2, streamEntry1, streamEntry2),
                            Sets.newHashSet(results));
      }
    });

  }

  @Test
  public void testSearchIncludesSystemEntities() throws InterruptedException, TransactionFailureException {
    // Use the same artifact in two different namespaces - system and ns2
    final co.cask.cdap.proto.id.ArtifactId sysArtifact = new co.cask.cdap.proto.id.ArtifactId(
      NamespaceId.SYSTEM.getNamespace(), "artifact", "1.0");
    final co.cask.cdap.proto.id.ArtifactId ns2Artifact = new co.cask.cdap.proto.id.ArtifactId(
      "ns2", "artifact", "1.0");
    final String multiWordKey = "multiword";
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(flow1, multiWordKey, multiWordValue);
        dataset.setProperty(sysArtifact, multiWordKey, multiWordValue);
        dataset.setProperty(ns2Artifact, multiWordKey, multiWordValue);
      }
    });
    // perform the exact same multiword search in the 'ns1' namespace. It should return the system artifact along with
    // matched entities in the 'ns1' namespace
    final MetadataEntry flowMultiWordEntry = new MetadataEntry(flow1, multiWordKey, multiWordValue);
    final MetadataEntry systemArtifactEntry = new MetadataEntry(sysArtifact, multiWordKey, multiWordValue);
    final MetadataEntry ns2ArtifactEntry = new MetadataEntry(ns2Artifact, multiWordKey, multiWordValue);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> results = dataset.search("ns1", "aV5", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(Sets.newHashSet(flowMultiWordEntry, systemArtifactEntry), Sets.newHashSet(results));
        // search only programs - should only return flow
        results = dataset.search("ns1", multiWordKey + MetadataDataset.KEYVALUE_SEPARATOR + "aV5",
                                 ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        Assert.assertEquals(ImmutableList.of(flowMultiWordEntry), results);
        // search only artifacts - should only return system artifact
        results = dataset.search("ns1", multiWordKey + MetadataDataset.KEYVALUE_SEPARATOR + multiWordValue,
                                 ImmutableSet.of(MetadataSearchTargetType.ARTIFACT));
        // this query returns the system artifact 4 times, since the dataset returns a list with duplicates for scoring
        // purposes. Convert to a Set for comparison.
        Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));
        // search all entities in namespace 'ns2' - should return the system artifact and the same artifact in ns2
        results = dataset.search("ns2", multiWordKey + MetadataDataset.KEYVALUE_SEPARATOR + "aV4",
                                 ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(Sets.newHashSet(systemArtifactEntry, ns2ArtifactEntry), Sets.newHashSet(results));
        // search only programs in a namespace 'ns2'. Should return empty
        results = dataset.search("ns2", "aV*", ImmutableSet.of(MetadataSearchTargetType.PROGRAM));
        Assert.assertTrue(results.isEmpty());
        // search all entities in namespace 'ns3'. Should return only the system artifact
        results = dataset.search("ns3", "av*", ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));
        // search the system namespace for all entities. Should return only the system artifact
        results = dataset.search(NamespaceId.SYSTEM.getEntityName(), "av*",
                                 ImmutableSet.of(MetadataSearchTargetType.ALL));
        Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));
      }
    });
    // clean up
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.removeProperties(flow1);
        dataset.removeProperties(sysArtifact);
      }
    });
  }

  @Test
  public void testUpdateSearch() throws Exception {
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(flow1, "key1", "value1");
        dataset.setProperty(flow1, "key2", "value2");
        dataset.addTags(flow1, "tag1", "tag2");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(ImmutableList.of(new MetadataEntry(flow1, "key1", "value1")),
                            dataset.search(flow1.getNamespace(), "value1",
                                           ImmutableSet.<MetadataSearchTargetType>of()));
        Assert.assertEquals(ImmutableList.of(new MetadataEntry(flow1, "key2", "value2")),
                            dataset.search(flow1.getNamespace(), "value2",
                                           ImmutableSet.<MetadataSearchTargetType>of()));
        Assert.assertEquals(ImmutableList.of(new MetadataEntry(flow1, MetadataDataset.TAGS_KEY, "tag1,tag2")),
                            dataset.search(flow1.getNamespace(), "tag2",
                                           ImmutableSet.<MetadataSearchTargetType>of()));
      }
    });

    // Update key1
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(flow1, "key1", "value3");
        dataset.removeProperties(flow1, "key2");
        dataset.removeTags(flow1, "tag2");
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Searching for value1 should be empty
        Assert.assertEquals(ImmutableList.of(),
                            dataset.search(flow1.getNamespace(), "value1",
                                           ImmutableSet.<MetadataSearchTargetType>of()));
        // Instead key1 has value value3 now
        Assert.assertEquals(ImmutableList.of(new MetadataEntry(flow1, "key1", "value3")),
                            dataset.search(flow1.getNamespace(), "value3",
                                           ImmutableSet.<MetadataSearchTargetType>of()));
        // key2 was deleted
        Assert.assertEquals(ImmutableList.of(),
                            dataset.search(flow1.getNamespace(), "value2",
                                           ImmutableSet.<MetadataSearchTargetType>of()));
        // tag2 was deleted
        Assert.assertEquals(ImmutableList.of(),
                            dataset.search(flow1.getNamespace(), "tag2", ImmutableSet.<MetadataSearchTargetType>of()));
        Assert.assertEquals(ImmutableList.of(new MetadataEntry(flow1, MetadataDataset.TAGS_KEY, "tag1")),
                            dataset.search(flow1.getNamespace(), "tag1", ImmutableSet.<MetadataSearchTargetType>of()));
      }
    });
  }

//  @Test
//  public void testAdvancedSearch() throws Exception {
//    txnl.execute(new TransactionExecutor.Subroutine() {
//      @Override
//      public void apply() throws Exception {
//        dataset.setProperty(flow1, "key1", "value1");
//        dataset.setProperty(flow1, "key2", "value2");
//        dataset.addTags(flow1, "tag1", "tag2");
//        dataset.setProperty(app1, "key", "value2");
//        dataset.addTags(app1, "tag1");
//      }
//    });
//    final AtomicReference<ImmutablePair<List<MetadataEntry>, List<MetadataEntry>>> results = new AtomicReference<>();
//    // search with 1 cursor
//    txnl.execute(new TransactionExecutor.Subroutine() {
//      @Override
//      public void apply() throws Exception {
//         results.set(
//           dataset.search(flow1.getNamespace(), "val*", Collections.<MetadataSearchTargetType>emptySet(), 1L,
// 1, 1, null)
//         );
//      }
//    });
//    // even though 3 results match the search query, we have only requested 1 cursor, so we get only 2
//    Assert.assertEquals(2, results.get().getFirst().size());
//    Assert.assertEquals(1, results.get().getSecond().size());
//    // search with 2 cursors
//    txnl.execute(new TransactionExecutor.Subroutine() {
//      @Override
//      public void apply() throws Exception {
//        results.set(
//          dataset.search(flow1.getNamespace(), "val*", Collections.<MetadataSearchTargetType>emptySet(), 1L, 1,
// 2, null)
//        );
//      }
//    });
//    Assert.assertEquals(3, results.get().getFirst().size());
//    Assert.assertEquals(2, results.get().getSecond().size());
//    // provide a cursor to start from
//    final MetadataEntry cursor1 = results.get().getSecond().get(0);
//    txnl.execute(new TransactionExecutor.Subroutine() {
//      @Override
//      public void apply() throws Exception {
//        results.set(
//          dataset.search(flow1.getNamespace(), "val*", Collections.<MetadataSearchTargetType>emptySet(), 1L, 1,
// 2, cursor1)
//        );
//      }
//    });
//    Assert.assertEquals(2, results.get().getFirst().size());
//    // two cursors are requested, but only one more is available, starting from the provided cursor
//    Assert.assertEquals(1, results.get().getSecond().size());
//  }

  @Test
  public void testAdvancedSearch() throws Exception {
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(flow1, "key1", "value1");
        dataset.setProperty(flow1, "key2", "value2");
        dataset.addTags(flow1, "tag1", "tag2");
        dataset.setProperty(app1, "key", "value2");
        dataset.addTags(app1, "tag1");
      }
    });
    List<MetadataEntry> allThreeFlows = ImmutableList.of(
      new MetadataEntry(flow1, "key1", "value1"),
      new MetadataEntry(flow1, "key2", "value2"),
      new MetadataEntry(app1, "key", "value2")
    );
    List<MetadataEntry> firstOnly = ImmutableList.of(
      new MetadataEntry(flow1, "key1", "value1")
    );
    List<MetadataEntry> firstTwo = ImmutableList.of(
      new MetadataEntry(flow1, "key1", "value1"),
      new MetadataEntry(flow1, "key2", "value2")
    );
    final AtomicReference<List<MetadataEntry>> results = new AtomicReference<>();
    // search with numResults = 1
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        results.set(
          dataset.search(flow1.getNamespace(), "val*", Collections.<MetadataSearchTargetType>emptySet(), 1L)
        );
      }
    });
    // search with limit numResults = 2
    Assert.assertEquals(firstOnly, results.get());
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        results.set(
          dataset.search(flow1.getNamespace(), "val*", Collections.<MetadataSearchTargetType>emptySet(), 2L)
        );
      }
    });
    Assert.assertEquals(firstTwo, results.get());
    // make the same query again. Should return the exact same result
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        results.set(
          dataset.search(flow1.getNamespace(), "val*", Collections.<MetadataSearchTargetType>emptySet(), 2L)
        );
      }
    });
    Assert.assertEquals(firstTwo, results.get());
    // search with numResults = 3.
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        results.set(
          dataset.search(flow1.getNamespace(), "val*", Collections.<MetadataSearchTargetType>emptySet(), 4L)
        );
      }
    });
    Assert.assertEquals(allThreeFlows, results.get());
    // search with numResults = Long.MAX_VALUE. Should return all all 3
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        results.set(
          dataset.search(flow1.getNamespace(), "val*", Collections.<MetadataSearchTargetType>emptySet(),
                         Long.MAX_VALUE)
        );
      }
    });
    Assert.assertEquals(allThreeFlows, results.get());
    // search with a composite query. Should return numComponents * numResults
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        results.set(
          dataset.search(flow1.getNamespace(), "val* tag*", Collections.<MetadataSearchTargetType>emptySet(), 1L)
        );
      }
    });
    // requested only one result, but get two since its a composite query
    Assert.assertEquals(2, results.get().size());
  }

  @Test
  public void testMultiGet() throws Exception {
    final Map<NamespacedEntityId, Metadata> allMetadata = new HashMap<>();
    allMetadata.put(flow1, new Metadata(flow1,
                                        ImmutableMap.of("key1", "value1", "key2", "value2"),
                                        ImmutableSet.of("tag1", "tag2", "tag3")));
    allMetadata.put(dataset1, new Metadata(dataset1,
                                           ImmutableMap.of("key10", "value10", "key11", "value11"),
                                           ImmutableSet.<String>of()));
    allMetadata.put(app1, new Metadata(app1,
                                       ImmutableMap.of("key20", "value20", "key21", "value21"),
                                       ImmutableSet.<String>of()));
    allMetadata.put(stream1, new Metadata(stream1,
                                          ImmutableMap.of("key30", "value30", "key31", "value31", "key32", "value32"),
                                          ImmutableSet.<String>of()));
    allMetadata.put(artifact1, new Metadata(artifact1,
                                            ImmutableMap.of("key40", "value41"),
                                            ImmutableSet.<String>of()));
    allMetadata.put(view1, new Metadata(view1,
                                        ImmutableMap.of("key50", "value50", "key51", "value51"),
                                        ImmutableSet.of("tag51")));

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Map.Entry<NamespacedEntityId, Metadata> entry : allMetadata.entrySet()) {
          Metadata metadata = entry.getValue();
          for (Map.Entry<String, String> props : metadata.getProperties().entrySet()) {
            dataset.setProperty(metadata.getEntityId(), props.getKey(), props.getValue());
          }
          dataset.addTags(metadata.getEntityId(), metadata.getTags().toArray(new String[metadata.getTags().size()]));
        }
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        ImmutableSet<Metadata> expected =
          ImmutableSet.<Metadata>builder()
            .add(allMetadata.get(flow1))
            .add(allMetadata.get(app1))
            .build();
        Assert.assertEquals(expected, dataset.getMetadata(ImmutableSet.of(flow1, app1)));

        expected =
          ImmutableSet.<Metadata>builder()
            .add(allMetadata.get(view1))
            .add(allMetadata.get(stream1))
            .add(allMetadata.get(dataset1))
            .add(allMetadata.get(artifact1))
            .build();
        Assert.assertEquals(expected, dataset.getMetadata(ImmutableSet.of(view1, stream1, dataset1, artifact1)));

        expected =
          ImmutableSet.<Metadata>builder()
            .add(allMetadata.get(artifact1))
            .build();
        Assert.assertEquals(expected, dataset.getMetadata(ImmutableSet.of(artifact1)));

        expected = ImmutableSet.of();
        Assert.assertEquals(expected, dataset.getMetadata(ImmutableSet.<NamespacedEntityId>of()));
      }
    });
  }

  @Test
  public void testDelete() throws Exception {
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(flow1, "key1", "value1");
        dataset.setProperty(flow1, "key2", "value2");
        dataset.addTags(flow1, "tag1", "tag2");

        dataset.setProperty(app1, "key10", "value10");
        dataset.setProperty(app1, "key12", "value12");
        dataset.addTags(app1, "tag11", "tag12");
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"), dataset.getProperties(flow1));
        Assert.assertEquals(ImmutableSet.of("tag1", "tag2"), dataset.getTags(flow1));
        Assert.assertEquals(ImmutableMap.of("key10", "value10", "key12", "value12"), dataset.getProperties(app1));
        Assert.assertEquals(ImmutableSet.of("tag11", "tag12"), dataset.getTags(app1));
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Delete all tags for flow1, and delete all properties for app1
        dataset.removeTags(flow1);
        dataset.removeProperties(app1);
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"), dataset.getProperties(flow1));
        Assert.assertEquals(ImmutableSet.of(), dataset.getTags(flow1));
        Assert.assertEquals(ImmutableMap.of(), dataset.getProperties(app1));
        Assert.assertEquals(ImmutableSet.of("tag11", "tag12"), dataset.getTags(app1));
      }
    });
  }

  @Test
  public void testHistory() throws Exception {
    MetadataDataset dataset =
      getDataset(DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("testHistory"));

    doTestHistory(dataset, flow1, "f_");
    doTestHistory(dataset, app1, "a_");
    doTestHistory(dataset, dataset1, "d_");
    doTestHistory(dataset, stream1, "s_");
  }

  @Test
  public void testIndexRebuilding() throws Exception {
    final MetadataDataset dataset =
      getDataset(DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("testIndexRebuilding"));
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(flow1, "flowKey", "flowValue", new ReversingIndexer());
        dataset.setProperty(dataset1, "datasetKey", "datasetValue", new ReversingIndexer());
      }
    });
    final String namespaceId = flow1.getNamespace();
    final Set<MetadataSearchTargetType> targetTypes = Collections.singleton(MetadataSearchTargetType.ALL);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> searchResults = dataset.search(namespaceId, "flowValue", targetTypes);
        Assert.assertTrue(searchResults.isEmpty());
        searchResults = dataset.search(namespaceId, "flowKey:flow*", targetTypes);
        Assert.assertTrue(searchResults.isEmpty());
        searchResults = dataset.search(namespaceId, "datasetValue", targetTypes);
        Assert.assertTrue(searchResults.isEmpty());
        searchResults = dataset.search(namespaceId, "datasetKey:dataset*", targetTypes);
        Assert.assertTrue(searchResults.isEmpty());
      }
    });
    final AtomicReference<byte[]> startRowKeyForNextBatch = new AtomicReference<>();
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Re-build indexes. Now the default indexer should be used
        startRowKeyForNextBatch.set(dataset.rebuildIndexes(null, 1));
        Assert.assertNotNull(startRowKeyForNextBatch.get());
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> flowSearchResults = dataset.search(namespaceId, "flowValue", targetTypes);
        List<MetadataEntry> dsSearchResults = dataset.search(namespaceId, "datasetValue", targetTypes);
        if (!flowSearchResults.isEmpty()) {
          Assert.assertEquals(1, flowSearchResults.size());
          flowSearchResults = dataset.search(namespaceId, "flowKey:flow*", targetTypes);
          Assert.assertEquals(1, flowSearchResults.size());
          Assert.assertTrue(dsSearchResults.isEmpty());
          dsSearchResults = dataset.search(namespaceId, "datasetKey:dataset*", targetTypes);
          Assert.assertTrue(dsSearchResults.isEmpty());
        } else {
          flowSearchResults = dataset.search(namespaceId, "flowKey:flow*", targetTypes);
          Assert.assertTrue(flowSearchResults.isEmpty());
          Assert.assertEquals(1, dsSearchResults.size());
          dsSearchResults = dataset.search(namespaceId, "datasetKey:dataset*", targetTypes);
          Assert.assertEquals(1, dsSearchResults.size());
        }
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        startRowKeyForNextBatch.set(dataset.rebuildIndexes(startRowKeyForNextBatch.get(), 1));
        Assert.assertNull(startRowKeyForNextBatch.get());
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> searchResults = dataset.search(namespaceId, "flowValue", targetTypes);
        Assert.assertEquals(1, searchResults.size());
        searchResults = dataset.search(namespaceId, "flowKey:flow*", targetTypes);
        Assert.assertEquals(1, searchResults.size());
        searchResults = dataset.search(namespaceId, "datasetValue", targetTypes);
        Assert.assertEquals(1, searchResults.size());
        searchResults = dataset.search(namespaceId, "datasetKey:dataset*", targetTypes);
        Assert.assertEquals(1, searchResults.size());
      }
    });
  }

  @Test
  public void testIndexDeletion() throws Exception {
    final MetadataDataset dataset =
      getDataset(DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("testIndexRebuilding"));
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(flow1, "flowKey", "flowValue");
        dataset.setProperty(dataset1, "datasetKey", "datasetValue");
      }
    });
    final String namespaceId = flow1.getNamespace();
    final Set<MetadataSearchTargetType> targetTypes = Collections.singleton(MetadataSearchTargetType.ALL);
    final MetadataEntry expectedFlowEntry = new MetadataEntry(flow1, "flowKey", "flowValue");
    final MetadataEntry expectedDatasetEntry = new MetadataEntry(dataset1, "datasetKey", "datasetValue");
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<MetadataEntry> searchResults = dataset.search(namespaceId, "flowValue", targetTypes);
        Assert.assertEquals(ImmutableList.of(expectedFlowEntry), searchResults);
        searchResults = dataset.search(namespaceId, "flowKey:flow*", targetTypes);
        Assert.assertEquals(ImmutableList.of(expectedFlowEntry), searchResults);
        searchResults = dataset.search(namespaceId, "datasetValue", targetTypes);
        Assert.assertEquals(ImmutableList.of(expectedDatasetEntry), searchResults);
        searchResults = dataset.search(namespaceId, "datasetKey:dataset*", targetTypes);
        Assert.assertEquals(ImmutableList.of(expectedDatasetEntry), searchResults);
      }
    });
    // delete indexes
    // 4 indexes should have been deleted - flowValue, flowKey:flowValue, datasetValue, datasetKey:datasetValue
    for (int i = 0; i < 4; i++) {
      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Assert.assertEquals(1, dataset.deleteAllIndexes(1));
        }
      });
    }
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(0, dataset.deleteAllIndexes(1));
      }
    });
  }

  private void doTestHistory(final MetadataDataset dataset, final NamespacedEntityId targetId, final String prefix)
    throws Exception {
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);

    // Metadata change history keyed by time in millis the change was made
    final Map<Long, Metadata> expected = new HashMap<>();
    // No history for targetId at the beginning
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Metadata completeRecord = new Metadata(targetId);
        expected.put(System.currentTimeMillis(), completeRecord);
        // Get history for targetId, should be empty
        Assert.assertEquals(ImmutableSet.of(completeRecord),
                            dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis()));
        // Also, the metadata itself should be equal to the last recorded snapshot
        Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                   System.currentTimeMillis())),
                            new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
      }
    });

    // Since the key to expected map is time in millis, sleep for a millisecond to make sure the key is distinct
    TimeUnit.MILLISECONDS.sleep(1);

    // Add first record
    final Metadata completeRecord =
      new Metadata(targetId, toProps(prefix, "k1", "v1"), toTags(prefix, "t1", "t2"));
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        addMetadataHistory(dataset, completeRecord);
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        long time = System.currentTimeMillis();
        expected.put(time, completeRecord);
        // Since this is the first record, history should be the same as what was added.
        Assert.assertEquals(ImmutableSet.of(completeRecord),
                            dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
        // Also, the metadata itself should be equal to the last recorded snapshot
        Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                   System.currentTimeMillis())),
                            new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
      }
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Add a new property and a tag
        dataset.setProperty(targetId, prefix + "k2", "v2");
        dataset.addTags(targetId, prefix + "t3");
      }
    });
    // Save the complete metadata record at this point
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Metadata completeRecord = new Metadata(targetId, toProps(prefix, "k1", "v1", "k2", "v2"),
                                               toTags(prefix, "t1", "t2", "t3"));
        long time = System.currentTimeMillis();
        expected.put(time, completeRecord);
        // Assert the history record with the change
        Assert.assertEquals(ImmutableSet.of(completeRecord),
                            dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
        // Also, the metadata itself should be equal to the last recorded snapshot
        Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                   System.currentTimeMillis())),
                            new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
      }
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Add another property and a tag
        dataset.setProperty(targetId, prefix + "k3", "v3");
        dataset.addTags(targetId, prefix + "t4");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Save the complete metadata record at this point
        Metadata completeRecord = new Metadata(targetId, toProps(prefix, "k1", "v1", "k2", "v2", "k3", "v3"),
                                               toTags(prefix, "t1", "t2", "t3", "t4"));
        long time = System.currentTimeMillis();
        expected.put(time, completeRecord);
        // Assert the history record with the change
        Assert.assertEquals(ImmutableSet.of(completeRecord),
                            dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
        // Also, the metadata itself should be equal to the last recorded snapshot
        Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                   System.currentTimeMillis())),
                            new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
      }
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Add the same property and tag as second time
        dataset.setProperty(targetId, prefix + "k2", "v2");
        dataset.addTags(targetId, prefix + "t3");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Save the complete metadata record at this point
        Metadata completeRecord = new Metadata(targetId, toProps(prefix, "k1", "v1", "k2", "v2", "k3", "v3"),
                                               toTags(prefix, "t1", "t2", "t3", "t4"));
        long time = System.currentTimeMillis();
        expected.put(time, completeRecord);
        // Assert the history record with the change
        Assert.assertEquals(ImmutableSet.of(completeRecord),
                            dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
        // Also, the metadata itself should be equal to the last recorded snapshot
        Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                   System.currentTimeMillis())),
                            new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
      }
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Remove a property and two tags
        dataset.removeProperties(targetId, prefix + "k2");
        dataset.removeTags(targetId, prefix + "t4");
        dataset.removeTags(targetId, prefix + "t2");
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Save the complete metadata record at this point
        Metadata completeRecord = new Metadata(targetId, toProps(prefix, "k1", "v1", "k3", "v3"),
                                               toTags(prefix, "t1", "t3"));
        long time = System.currentTimeMillis();
        expected.put(time, completeRecord);
        // Assert the history record with the change
        Assert.assertEquals(ImmutableSet.of(completeRecord),
                            dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
        // Also, the metadata itself should be equal to the last recorded snapshot
        Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                   System.currentTimeMillis())),
                            new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
      }
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Remove all properties and all tags
        dataset.removeProperties(targetId);
        dataset.removeTags(targetId);
      }
    });
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Save the complete metadata record at this point
        Metadata completeRecord = new Metadata(targetId);
        long time = System.currentTimeMillis();
        expected.put(time, completeRecord);
        // Assert the history record with the change
        Assert.assertEquals(ImmutableSet.of(completeRecord),
                            dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
        // Also, the metadata itself should be equal to the last recorded snapshot
        Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                   System.currentTimeMillis())),
                            new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
      }
    });
    TimeUnit.MILLISECONDS.sleep(1);

    // Add one more property and a tag
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.setProperty(targetId, prefix + "k2", "v2");
        dataset.addTags(targetId, prefix + "t2");
      }
    });
    final Metadata lastCompleteRecord = new Metadata(targetId, toProps(prefix, "k2", "v2"),
                                                     toTags(prefix, "t2"));
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Save the complete metadata record at this point
        long time = System.currentTimeMillis();
        expected.put(time, lastCompleteRecord);
        // Assert the history record with the change
        Assert.assertEquals(ImmutableSet.of(lastCompleteRecord),
                            dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
        // Also, the metadata itself should be equal to the last recorded snapshot
        Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                   System.currentTimeMillis())),
                            new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
      }
    });
    TimeUnit.MILLISECONDS.sleep(1);

    // Now assert all history
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Map.Entry<Long, Metadata> entry : expected.entrySet()) {
          Assert.assertEquals(entry.getValue(),
                              getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), entry.getKey())));
        }
        // Asserting for current time should give the latest record
        Assert.assertEquals(ImmutableSet.of(lastCompleteRecord),
                            dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis()));
        // Also, the metadata itself should be equal to the last recorded snapshot
        Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                   System.currentTimeMillis())),
                            new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
      }
    });
  }

  private void addMetadataHistory(MetadataDataset dataset, Metadata record) {
    for (Map.Entry<String, String> entry : record.getProperties().entrySet()) {
      dataset.setProperty(record.getEntityId(), entry.getKey(), entry.getValue());
    }
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    dataset.addTags(record.getEntityId(), record.getTags().toArray(new String[0]));
  }

  private Map<String, String> toProps(String prefix, String k1, String v1) {
    return ImmutableMap.of(prefix + k1, v1);
  }

  private Map<String, String> toProps(String prefix, String k1, String v1, String k2, String v2) {
    return ImmutableMap.of(prefix + k1, v1, prefix + k2, v2);
  }

  private Map<String, String>
  toProps(String prefix, String k1, String v1, String k2, String v2, String k3, String v3) {
    return ImmutableMap.of(prefix + k1, v1, prefix + k2, v2, prefix + k3, v3);
  }

  private Set<String> toTags(String prefix, String... tags) {
    ImmutableSet.Builder<String> builder = new ImmutableSet.Builder<>();
    for (String tag : tags) {
      builder.add(prefix + tag);
    }
    return builder.build();
  }

  private <T> T getFirst(Iterable<T> iterable) {
    Assert.assertEquals(1, Iterables.size(iterable));
    return iterable.iterator().next();
  }

  private static MetadataDataset getDataset(DatasetId instance) throws Exception {
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), instance,
                                           MetadataDataset.class.getName(),
                                           DatasetProperties.EMPTY, null, null);
  }

  private static final class ReversingIndexer implements Indexer {

    @Override
    public Set<String> getIndexes(MetadataEntry entry) {
      return ImmutableSet.of(reverse(entry.getKey()), reverse(entry.getValue()));
    }

    private String reverse(String toReverse) {
      return new StringBuilder(toReverse).reverse().toString();
    }
  }
}
