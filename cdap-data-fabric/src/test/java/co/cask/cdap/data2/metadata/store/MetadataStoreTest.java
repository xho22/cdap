/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.store;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.audit.InMemoryAuditPublisher;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.Metadata;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link MetadataStore}
 */
public class MetadataStoreTest {
  private static final Map<String, String> EMPTY_PROPERTIES = Collections.emptyMap();
  private static final Set<String> EMPTY_TAGS = Collections.emptySet();
  private static final Map<MetadataScope, Metadata> EMPTY_USER_METADATA =
    ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, EMPTY_TAGS));

  private final ApplicationId app = NamespaceId.DEFAULT.app("app");
  private final ProgramId flow = app.flow("flow");
  private final DatasetId dataset = NamespaceId.DEFAULT.dataset("ds");
  private final StreamId stream = NamespaceId.DEFAULT.stream("stream");
  private final Set<String> datasetTags = ImmutableSet.of("dTag");
  private final Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue");
  private final Set<String> appTags = ImmutableSet.of("aTag");
  private final Map<String, String> streamProperties = ImmutableMap.of("stKey", "stValue");
  private final Map<String, String> updatedStreamProperties = ImmutableMap.of("stKey", "stV");
  private final Set<String> flowTags = ImmutableSet.of("fTag");

  private final AuditMessage auditMessage1 = new AuditMessage(
    0, dataset, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA, ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, datasetTags)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage2 = new AuditMessage(
    0, app, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA, ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, EMPTY_TAGS)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage3 = new AuditMessage(
    0, app, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, EMPTY_TAGS)),
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, appTags)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage4 = new AuditMessage(
    0, stream, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamProperties, EMPTY_TAGS)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage5 = new AuditMessage(
    0, stream, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamProperties, EMPTY_TAGS)),
      EMPTY_USER_METADATA, EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage6 = new AuditMessage(
    0, stream, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamProperties, EMPTY_TAGS)),
      ImmutableMap.of(MetadataScope.USER, new Metadata(updatedStreamProperties, EMPTY_TAGS)),
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamProperties, EMPTY_TAGS))
    )
  );
  private final AuditMessage auditMessage7 = new AuditMessage(
    0, flow, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, flowTags)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage8 = new AuditMessage(
    0, flow, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, flowTags)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, flowTags))
    )
  );
  private final AuditMessage auditMessage9 = new AuditMessage(
    0, dataset, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, datasetTags)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, datasetTags))
    )
  );
  private final AuditMessage auditMessage10 = new AuditMessage(
    0, stream, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(updatedStreamProperties, EMPTY_TAGS)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(updatedStreamProperties, EMPTY_TAGS))
    )
  );
  private final AuditMessage auditMessage11 = new AuditMessage(
    0, app, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, appTags)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, appTags))
    )
  );
  private final List<AuditMessage> expectedAuditMessages = ImmutableList.of(
    auditMessage1, auditMessage2, auditMessage3, auditMessage4, auditMessage5, auditMessage6, auditMessage7,
    auditMessage8, auditMessage9, auditMessage10, auditMessage11
  );

  private static CConfiguration cConf;
  private static TransactionManager txManager;
  private static MetadataStore store;
  private static InMemoryAuditPublisher auditPublisher;

  @BeforeClass
  public static void setup() throws IOException {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      Modules.override(
        new DataSetsModules().getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          // Need the distributed metadata store.
          bind(MetadataStore.class).to(DefaultMetadataStore.class);
        }
      }),
      new LocationRuntimeModule().getInMemoryModules(),
      new TransactionInMemoryModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AuditModule().getInMemoryModules()
    );
    cConf = injector.getInstance(CConfiguration.class);
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    store = injector.getInstance(MetadataStore.class);
    auditPublisher = injector.getInstance(InMemoryAuditPublisher.class);
  }

  @Before
  public void clearAudit() throws Exception {
    auditPublisher.popMessages();
  }

  @Test
  public void testPublishing() throws InterruptedException {
    generateMetadataUpdates();

    // Audit messages for metadata changes
    List<AuditMessage> actualAuditMessages = new ArrayList<>();
    for (AuditMessage auditMessage : auditPublisher.popMessages()) {
      // Ignore system audit messages
      if (auditMessage.getEntityId() instanceof NamespacedEntityId) {
        String systemNs = NamespaceId.SYSTEM.getNamespace();
        if (!((NamespacedEntityId) auditMessage.getEntityId()).getNamespace().equals(systemNs)) {
          actualAuditMessages.add(auditMessage);
        }
      }
    }
    Assert.assertEquals(expectedAuditMessages, actualAuditMessages);
  }

  @Test
  public void testPublishingDisabled() throws InterruptedException {
    boolean auditEnabled = cConf.getBoolean(Constants.Audit.ENABLED);
    cConf.setBoolean(Constants.Audit.ENABLED, false);
    generateMetadataUpdates();
    try {
      List<AuditMessage> publishedAuditMessages = auditPublisher.popMessages();
      Assert.fail(String.format("Expected no changes to be published, but found %d changes: %s.",
                                publishedAuditMessages.size(), publishedAuditMessages));
    } catch (AssertionError e) {
      // expected
    }
    // reset config
    cConf.setBoolean(Constants.Audit.ENABLED, auditEnabled);
  }

  @Test
  public void testSearchWeight() throws Exception {
    ProgramId flow1 = new ProgramId("ns1", "app1", ProgramType.FLOW, "flow1");
    StreamId stream1 = new StreamId("ns1", "s1");
    DatasetId dataset1 = new DatasetId("ns1", "ds1");

    // Add metadata
    String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    Map<String, String> flowUserProps = ImmutableMap.of("key1", "value1",
                                                                 "key2", "value2",
                                                                 "multiword", multiWordValue);
    Map<String, String> flowSysProps = ImmutableMap.of("sysKey1", "sysValue1");
    Set<String> flowUserTags = ImmutableSet.of("tag1", "tag2");
    Set<String> streamUserTags = ImmutableSet.of("tag3", "tag4");
    Set<String> flowSysTags = ImmutableSet.of("sysTag1");
    store.setProperties(MetadataScope.USER, flow1, flowUserProps);
    store.setProperties(MetadataScope.SYSTEM, flow1, flowSysProps);
    store.addTags(MetadataScope.USER, flow1, flowUserTags.toArray(new String[flowUserTags.size()]));
    store.addTags(MetadataScope.SYSTEM, flow1, flowSysTags.toArray(new String[flowSysTags.size()]));
    store.addTags(MetadataScope.USER, stream1, streamUserTags.toArray(new String[streamUserTags.size()]));
    store.removeTags(MetadataScope.USER, stream1, streamUserTags.toArray(new String[streamUserTags.size()]));
    store.setProperties(MetadataScope.USER, stream1, flowUserProps);
    store.removeProperties(MetadataScope.USER, stream1, "key1", "key2", "multiword");

    Map<String, String> streamUserProps = ImmutableMap.of("sKey1", "sValue1 sValue2",
                                                                   "Key1", "Value1");
    store.setProperties(MetadataScope.USER, stream1, streamUserProps);

    Map<String, String> datasetUserProps = ImmutableMap.of("sKey1", "sValuee1 sValuee2");
    store.setProperties(MetadataScope.USER, dataset1, datasetUserProps);

    // Test score and metadata match
    List<MetadataSearchResultRecord> actual = Lists.newArrayList(
      store.searchMetadata("ns1", "value1 multiword:av2", Long.MAX_VALUE));

    Map<MetadataScope, Metadata> expectedFlowMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(flowUserProps, flowUserTags),
                      MetadataScope.SYSTEM, new Metadata(flowSysProps, flowSysTags));
    Map<MetadataScope, Metadata> expectedStreamMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamUserProps, Collections.<String>emptySet()));
    Map<MetadataScope, Metadata> expectedDatasetMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(datasetUserProps, Collections.<String>emptySet()));
    List<MetadataSearchResultRecord> expected =
      Lists.newArrayList(
        new MetadataSearchResultRecord(flow1,
                                       expectedFlowMetadata),
        new MetadataSearchResultRecord(stream1,
                                       expectedStreamMetadata)
      );
    Assert.assertEquals(expected, actual);

    actual = Lists.newArrayList(store.searchMetadata("ns1", "value1 sValue*", Long.MAX_VALUE));
    expected = Lists.newArrayList(
      new MetadataSearchResultRecord(stream1,
                                     expectedStreamMetadata),
      new MetadataSearchResultRecord(dataset1,
                                     expectedDatasetMetadata),
      new MetadataSearchResultRecord(flow1,
                                     expectedFlowMetadata)
    );
    Assert.assertEquals(expected, actual);

    actual = Lists.newArrayList(store.searchMetadata("ns1", "*", Long.MAX_VALUE));
    Assert.assertTrue(actual.containsAll(expected));
  }

  @AfterClass
  public static void teardown() {
    txManager.stopAndWait();
  }

  private void generateMetadataUpdates() {
    store.addTags(MetadataScope.USER, dataset, datasetTags.iterator().next());
    store.setProperties(MetadataScope.USER, app, appProperties);
    store.addTags(MetadataScope.USER, app, appTags.iterator().next());
    store.setProperties(MetadataScope.USER, stream, streamProperties);
    store.setProperties(MetadataScope.USER, stream, streamProperties);
    store.setProperties(MetadataScope.USER, stream, updatedStreamProperties);
    store.addTags(MetadataScope.USER, flow, flowTags.iterator().next());
    store.removeTags(MetadataScope.USER, flow);
    store.removeTags(MetadataScope.USER, dataset, datasetTags.iterator().next());
    store.removeProperties(MetadataScope.USER, stream);
    store.removeMetadata(MetadataScope.USER, app);
  }
}
