/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import javax.annotation.Nullable;

/**
 * Common utilities for dealing with HBase.
 */
public abstract class HBaseTableUtil {

  public static final String CDAP_VERSION = "cdap.version";

  /**
   * Represents the compression types supported for HBase tables.
   */
  public enum CompressionType {
    LZO, SNAPPY, GZIP, NONE
  }

  /**
   * Represents the bloom filter types supported for HBase tables.
   */
  public enum BloomType {
    ROW, ROWCOL, NONE
  }

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableUtil.class);

  public static final long MAX_CREATE_TABLE_WAIT = 5000L;    // Maximum wait of 5 seconds for table creation.

  // 4Mb
  public static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  private static final int COPY_BUFFER_SIZE = 0x1000;    // 4K
  private static final CompressionType DEFAULT_COMPRESSION_TYPE = CompressionType.SNAPPY;
  public static final String CFG_HBASE_TABLE_COMPRESSION = "hbase.table.compression.default";


  protected String tablePrefix;
  protected NamespaceQueryAdmin namespaceQueryAdmin;

  public void setCConf(CConfiguration cConf) {
    this.tablePrefix = getTablePrefix(cConf);
  }

  public void setNamespaceQueryAdmin(NamespaceQueryAdmin namespaceQueryAdmin) {
    if (namespaceQueryAdmin != null) {
      this.namespaceQueryAdmin = namespaceQueryAdmin;
    }
  }

  /**
   * Returns a map of HBase to CDAP namespace. This is required when we want to report metrics for HBase tables where
   * it is run a separate service and reads the table metrics and reports it. There we need to translate the hbase
   * namespace to cdap namespace for metrics to make sense from CDAP perspective. This is also used during upgrade
   * step where we want to construct the correct {@link DatasetAdmin} for each dataset.
   *
   * @return map of hbase namespace to cdap namespace
   * @throws IOException if there was an error getting the {@link NamespaceMeta} of all the namespaces
   */
  public Map<String, String> getHBaseToCDAPNamespaceMap() throws IOException {
    Map<String, String> reverseMap = new HashMap<>();
    if (namespaceQueryAdmin == null) {
      throw new IOException("NamespaceQueryAdmin is not set and a reverseLookupMap was requested.");
    }

    try {
      List<NamespaceMeta> namespaceMetas = namespaceQueryAdmin.list();
      for (NamespaceMeta namespaceMeta : namespaceMetas) {
        String hbaseNamespace = getHBaseNamespace(namespaceMeta);
        reverseMap.put(hbaseNamespace, namespaceMeta.getName());
      }
    } catch (Exception ex) {
      throw new IOException("NamespaceQueryAdmin lookup to list all NamespaceMetas failed", ex);
    }

    return ImmutableMap.copyOf(reverseMap);
  }

  public String getHBaseNamespace(NamespaceId namespaceId) throws IOException {
    // Convert CDAP Namespace to HBase namespace
    if (NamespaceId.SYSTEM.equals(namespaceId) || NamespaceId.CDAP.equals(namespaceId) ||
      NamespaceId.DEFAULT.equals(namespaceId)) {
      return toCDAPManagedHBaseNamespace(namespaceId);
    }

    if (namespaceQueryAdmin == null) {
      throw new IOException(String.format("NamespaceQueryAdmin is not set and a non-reserved namespace " +
                                            "lookup is requested. Namespace %s", namespaceId.getNamespace()));
    }

    try {
      return getHBaseNamespace(namespaceQueryAdmin.get(namespaceId));
    } catch (Exception ex) {
      throw new IOException(String.format("NamespaceQueryAdmin lookup to get NamespaceMeta failed. " +
                                            "Can't find mapping for %s", namespaceId.getNamespace()), ex);
    }
  }

  public String getHBaseNamespace(NamespaceMeta namespaceMeta) {
    if (!Strings.isNullOrEmpty(namespaceMeta.getConfig().getHbaseNamespace())) {
      return namespaceMeta.getConfig().getHbaseNamespace();
    }
    return toCDAPManagedHBaseNamespace(namespaceMeta.getNamespaceId());
  }

  public TableId createHTableId(NamespaceId namespace, String tableName) throws IOException {
    return TableId.from(getHBaseNamespace(namespace), tableName);
  }

  private String toCDAPManagedHBaseNamespace(NamespaceId namespace) {
    // Handle backward compatibility to not add the prefix for default namespace
    // TODO: CDAP-1601 - Conditional should be removed when we have a way to upgrade user datasets
    return NamespaceId.DEFAULT.getEntityName().equals(namespace.getNamespace()) ?
      namespace.getNamespace() : tablePrefix + "_" + namespace.getNamespace();
  }

  protected boolean isCDAPTable(HTableDescriptor hTableDescriptor) {
    // TODO: Once all system tables are upgraded to have CDAP_VERSION in their descriptor, we can then solely rely on
    // that key being present in the descriptor to identify a CDAP Table
    String tableName = hTableDescriptor.getNameAsString();
    String value = hTableDescriptor.getValue(CDAP_VERSION);
    return tableName.startsWith(tablePrefix + ".") || tableName.startsWith(tablePrefix + "_") ||
      !Strings.isNullOrEmpty(value);
  }

  /**
   * Create a hbase table if it does not exist. Deals with race conditions when two clients concurrently attempt to
   * create the table.
   * @param admin the hbase admin
   * @param tableId the {@link TableId} for the table to create
   * @param tableDescriptor hbase table descriptor for the new table
   */
  public void createTableIfNotExists(HBaseAdmin admin, TableId tableId,
                                     HTableDescriptor tableDescriptor) throws IOException {
    createTableIfNotExists(admin, tableId, tableDescriptor, null);
  }

  /**
   * Creates a hbase table if it does not exists. Same as calling
   * {@link #createTableIfNotExists(HBaseAdmin, TableId, HTableDescriptor, byte[][], long, TimeUnit)}
   * with timeout = {@link #MAX_CREATE_TABLE_WAIT} milliseconds.
   */
  public void createTableIfNotExists(HBaseAdmin admin, TableId tableId,
                                     HTableDescriptor tableDescriptor,
                                     @Nullable byte[][] splitKeys) throws IOException {
    createTableIfNotExists(admin, tableId, tableDescriptor, splitKeys,
                           MAX_CREATE_TABLE_WAIT, TimeUnit.MILLISECONDS);
  }

  /**
   * Create a hbase table if it does not exist. Deals with race conditions when two clients concurrently attempt to
   * create the table.
   * @param admin the hbase admin
   * @param tableId {@link TableId} representing the table
   * @param tableDescriptor hbase table descriptor for the new table
   * @param timeout Maximum time to wait for table creation.
   * @param timeoutUnit The TimeUnit for timeout.
   */
  public void createTableIfNotExists(HBaseAdmin admin, TableId tableId, HTableDescriptor tableDescriptor,
                                     @Nullable byte[][] splitKeys,
                                     long timeout, TimeUnit timeoutUnit) throws IOException {
    if (tableExists(admin, tableId)) {
      return;
    }
    setDefaultConfiguration(tableDescriptor, admin.getConfiguration());
    tableDescriptor = setVersion(tableDescriptor);
    tableDescriptor = setTablePrefix(tableDescriptor);
    try {
      LOG.debug("Attempting to create table '{}' if it does not exist", tableId);
      // HBaseAdmin.createTable can handle null splitKeys.
      admin.createTable(tableDescriptor, splitKeys);
      LOG.info("Table created '{}'", tableId);
      return;
    } catch (TableExistsException e) {
      // table may exist because someone else is creating it at the same
      // time. But it may not be available yet, and opening it might fail.
      LOG.debug("Table '{}' already exists.", tableId, e);
    }

    // Wait for table to materialize
    try {
      Stopwatch stopwatch = new Stopwatch();
      stopwatch.start();
      long sleepTime = timeoutUnit.toNanos(timeout) / 10;
      sleepTime = sleepTime <= 0 ? 1 : sleepTime;
      do {
        if (tableExists(admin, tableId)) {
          LOG.info("Table '{}' exists now. Assuming that another process concurrently created it.", tableId);
          return;
        } else {
          TimeUnit.NANOSECONDS.sleep(sleepTime);
        }
      } while (stopwatch.elapsedTime(timeoutUnit) < timeout);
    } catch (InterruptedException e) {
      LOG.warn("Sleeping thread interrupted.");
    }
    LOG.error("Table '{}' does not exist after waiting {} ms. Giving up.", tableId, MAX_CREATE_TABLE_WAIT);
  }

  // This is a workaround for unit-tests which should run even if compression is not supported
  // todo: this should be addressed on a general level: CDAP may use HBase cluster (or multiple at a time some of)
  //       which doesn't support certain compression type
  private void setDefaultConfiguration(HTableDescriptor tableDescriptor, Configuration conf) {
    String compression = conf.get(CFG_HBASE_TABLE_COMPRESSION, DEFAULT_COMPRESSION_TYPE.name());
    CompressionType compressionAlgo = CompressionType.valueOf(compression);
    for (HColumnDescriptor hcd : tableDescriptor.getColumnFamilies()) {
      setCompression(hcd, compressionAlgo);
      setBloomFilter(hcd, BloomType.ROW);
    }
  }

  public HTableDescriptor setVersion(HTableDescriptor tableDescriptor) {
    HTableDescriptorBuilder builder = buildHTableDescriptor(tableDescriptor);
    setVersion(builder);
    return builder.build();
  }

  public HTableDescriptor setTablePrefix(HTableDescriptor tableDescriptor) {
    HTableDescriptorBuilder builder = buildHTableDescriptor(tableDescriptor);
    builder.setValue(Constants.Dataset.TABLE_PREFIX, tablePrefix);
    return builder.build();
  }

  // For simplicity we allow max 255 splits per bucket for now
  private static final int MAX_SPLIT_COUNT_PER_BUCKET = 0xff;

  public static void setTablePrefix(HTableDescriptorBuilder tableDescriptorBuilder, CConfiguration cConf) {
    tableDescriptorBuilder.setValue(Constants.Dataset.TABLE_PREFIX, getTablePrefix(cConf));
  }

  private static String getTablePrefix(@Nullable CConfiguration cConf) {
    return cConf == null ? null : cConf.get(Constants.Dataset.TABLE_PREFIX);
  }

  public static void setVersion(HTableDescriptorBuilder tableDescriptorBuilder) {
    tableDescriptorBuilder.setValue(CDAP_VERSION, ProjectInfo.getVersion().toString());
  }

  public static ProjectInfo.Version getVersion(HTableDescriptor tableDescriptor) {
    return new ProjectInfo.Version(tableDescriptor.getValue(CDAP_VERSION));
  }

  public static byte[][] getSplitKeys(int splits, int buckets, AbstractRowKeyDistributor keyDistributor) {
    // "1" can be used for queue tables that we know are not "hot", so we do not pre-split in this case
    if (splits == 1) {
      return new byte[0][];
    }

    byte[][] bucketSplits = keyDistributor.getAllDistributedKeys(Bytes.EMPTY_BYTE_ARRAY);
    Preconditions.checkArgument(splits >= 1 && splits <= MAX_SPLIT_COUNT_PER_BUCKET * bucketSplits.length,
                                "Number of pre-splits should be in [1.." +
                                  MAX_SPLIT_COUNT_PER_BUCKET * bucketSplits.length + "] range");


    // Splits have format: <salt bucket byte><extra byte>. We use extra byte to allow more splits than buckets:
    // salt bucket bytes are usually sequential in which case we cannot insert any value in between them.

    int splitsPerBucket = (splits + buckets - 1) / buckets;
    splitsPerBucket = splitsPerBucket == 0 ? 1 : splitsPerBucket;

    byte[][] splitKeys = new byte[bucketSplits.length * splitsPerBucket - 1][];

    int prefixesPerSplitInBucket = (MAX_SPLIT_COUNT_PER_BUCKET + 1) / splitsPerBucket;

    for (int i = 0; i < bucketSplits.length; i++) {
      for (int k = 0; k < splitsPerBucket; k++) {
        if (i == 0 && k == 0) {
          // hbase will figure out first split
          continue;
        }
        int splitStartPrefix = k * prefixesPerSplitInBucket;
        int thisSplit = i * splitsPerBucket + k - 1;
        if (splitsPerBucket > 1) {
          splitKeys[thisSplit] = new byte[] {(byte) i, (byte) splitStartPrefix};
        } else {
          splitKeys[thisSplit] = new byte[] {(byte) i};
        }
      }
    }

    return splitKeys;
  }

  public static Location createCoProcessorJar(String filePrefix, Location jarDir,
                                              Iterable<? extends Class<? extends Coprocessor>> classes)
                                              throws IOException {
    StringBuilder buf = new StringBuilder();
    for (Class<? extends Coprocessor> c : classes) {
      buf.append(c.getName()).append(", ");
    }
    if (buf.length() == 0) {
      return null;
    }

    LOG.debug("Creating jar file for coprocessor classes: " + buf.toString());
    final Hasher hasher = Hashing.md5().newHasher();
    final byte[] buffer = new byte[COPY_BUFFER_SIZE];

    final Map<String, URL> dependentClasses = new HashMap<>();
    for (Class<? extends Coprocessor> clz : classes) {
      Dependencies.findClassDependencies(clz.getClassLoader(), new ClassAcceptor() {
        @Override
        public boolean accept(String className, final URL classUrl, URL classPathUrl) {
          // Assuming the endpoint and protocol class doesn't have dependencies
          // other than those comes with HBase, Java and fastutil.
          if (className.startsWith("co.cask") || className.startsWith("it.unimi.dsi.fastutil")
            || className.startsWith("org.apache.tephra")) {
            if (!dependentClasses.containsKey(className)) {
              dependentClasses.put(className, classUrl);
            }
            return true;
          }
          return false;
        }
      }, clz.getName());
    }

    if (!dependentClasses.isEmpty()) {
      LOG.debug("Adding " + dependentClasses.size() + " classes to jar");
      File jarFile = File.createTempFile(filePrefix, ".jar");
      try {
        try (JarOutputStream jarOutput = new JarOutputStream(new FileOutputStream(jarFile))) {
          for (Map.Entry<String, URL> entry : dependentClasses.entrySet()) {
            try {
              jarOutput.putNextEntry(new JarEntry(entry.getKey().replace('.', File.separatorChar) + ".class"));

              try (InputStream inputStream = entry.getValue().openStream()) {
                int len = inputStream.read(buffer);
                while (len >= 0) {
                  hasher.putBytes(buffer, 0, len);
                  jarOutput.write(buffer, 0, len);
                  len = inputStream.read(buffer);
                }
              }
            } catch (IOException e) {
              LOG.info("Error writing to jar", e);
              throw Throwables.propagate(e);
            }
          }
        }

        // Copy jar file into HDFS
        // Target path is the jarDir + jarMD5.jar
        final Location targetPath = jarDir.append("coprocessor-" + ProjectInfo.getVersion()
                                                    + "-" + hasher.hash().toString() + ".jar");

        // If the file exists and having same since, assume the file doesn't changed
        if (targetPath.exists() && targetPath.length() == jarFile.length()) {
          return targetPath;
        }

        // Copy jar file into filesystem
        if (!jarDir.mkdirs() && !jarDir.exists()) {
          throw new IOException("Fails to create directory: " + jarDir);
        }
        Files.copy(jarFile, new OutputSupplier<OutputStream>() {
          @Override
          public OutputStream getOutput() throws IOException {
            return targetPath.getOutputStream();
          }
        });
        return targetPath;
      } finally {
        jarFile.delete();
      }
    }
    // no dependent classes to add
    return null;
  }


  /**
   * Returns information for all coprocessor configured for the table.
   *
   * @return a Map from coprocessor class name to CoprocessorInfo
   */
  public static Map<String, CoprocessorInfo> getCoprocessorInfo(HTableDescriptor tableDescriptor) {
    Map<String, CoprocessorInfo> info = Maps.newHashMap();

    // Extract information about existing data janitor coprocessor
    // The following logic is copied from RegionCoprocessorHost in HBase
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry: tableDescriptor.getValues().entrySet()) {
      String key = Bytes.toString(entry.getKey().get()).trim();
      String spec = Bytes.toString(entry.getValue().get()).trim();

      if (!HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(key).matches()) {
        continue;
      }

      try {
        Matcher matcher = HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(spec);
        if (!matcher.matches()) {
          continue;
        }

        String className = matcher.group(2).trim();
        Path path = matcher.group(1).trim().isEmpty() ? null : new Path(matcher.group(1).trim());
        int priority = matcher.group(3).trim().isEmpty() ? Coprocessor.PRIORITY_USER
          : Integer.valueOf(matcher.group(3));
        String cfgSpec = null;
        try {
          cfgSpec = matcher.group(4);
        } catch (IndexOutOfBoundsException ex) {
          // ignore
        }

        Map<String, String> properties = Maps.newHashMap();
        if (cfgSpec != null) {
          cfgSpec = cfgSpec.substring(cfgSpec.indexOf('|') + 1);
          // do an explicit deep copy of the passed configuration
          Matcher m = HConstants.CP_HTD_ATTR_VALUE_PARAM_PATTERN.matcher(cfgSpec);
          while (m.find()) {
            properties.put(m.group(1), m.group(2));
          }
        }
        info.put(className, new CoprocessorInfo(className, path, priority, properties));
      } catch (Exception ex) {
        LOG.warn("Coprocessor attribute '{}' has invalid coprocessor specification '{}'", key, spec, ex);
      }
    }

    return info;
  }

  /**
   * Creates a new {@link HTable} which may contain an HBase namespace depending on the HBase version
   *
   * @param conf the hadoop configuration
   * @param tableId the {@link TableId} to create an {@link HTable} for
   * @return an {@link HTable} for the tableName in the namespace
   */
  public abstract HTable createHTable(Configuration conf, TableId tableId) throws IOException;

  /**
   * Creates a new {@link HTableDescriptorBuilder} which may contain an HBase namespace depending on the HBase version
   *
   * @param tableId the {@link TableId} to create an {@link HTableDescriptor} for
   * @return an {@link HTableDescriptorBuilder} for the table
   */
  public abstract HTableDescriptorBuilder buildHTableDescriptor(TableId tableId);

  /**
   * Creates a new {@link HTableDescriptorBuilder} which may contain an HBase namespace depending on the HBase version
   *
   * @param tableDescriptor the {@link HTableDescriptor} whose values should be copied
   * @return an {@link HTableDescriptorBuilder} for the table
   */
  public abstract HTableDescriptorBuilder buildHTableDescriptor(HTableDescriptor tableDescriptor);

  /**
   * Constructs a {@link HTableDescriptor} which may contain an HBase namespace for an existing table
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId the {@link TableId} to construct an {@link HTableDescriptor} for
   * @return an {@link HTableDescriptor} for the table
   * @throws IOException
   */
  public abstract HTableDescriptor getHTableDescriptor(HBaseAdmin admin, TableId tableId) throws IOException;

  /**
   * Checks if an HBase namespace already exists
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param namespace the namespace to check for existence
   * @throws IOException if an I/O error occurs during the operation
   */
  public abstract boolean hasNamespace(HBaseAdmin admin, String namespace) throws IOException;

  /**
   * Creates an HBase namespace, if it does not already exist
   * This method uses {@link Id.Namespace} here to cover for a future case when CDAP namespaces may push down namespace
   * properties to HBase
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param namespace the namespace to create
   * @throws IOException if an I/O error occurs during the operation
   */
  public abstract void createNamespaceIfNotExists(HBaseAdmin admin, String namespace) throws IOException;

  /**
   * Creates an HBase namespace, if it exists
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param namespace the namespace to delete
   * @throws IOException if an I/O error occurs during the operation
   */
  public abstract void  deleteNamespaceIfExists(HBaseAdmin admin, String namespace) throws IOException;

  /**
   * Disable an HBase table
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId {@link TableId} for the specified table
   * @throws IOException
   */
  public abstract void disableTable(HBaseAdmin admin, TableId tableId) throws IOException;

  /**
   * Enable an HBase table
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId {@link TableId} for the specified table
   * @throws IOException
   */
  public abstract void enableTable(HBaseAdmin admin, TableId tableId) throws IOException;

  /**
   * Check if an HBase table exists
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId {@link TableId} for the specified table
   * @throws IOException
   */
  public abstract boolean tableExists(HBaseAdmin admin, TableId tableId) throws IOException;

  /**
   * Delete an HBase table
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId {@link TableId} for the specified table
   * @throws IOException
   */
  public abstract void deleteTable(HBaseAdmin admin, TableId tableId) throws IOException;

  /**
   * Modify an HBase table
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableDescriptor the modified {@link HTableDescriptor}
   * @throws IOException
   */
  public abstract void modifyTable(HBaseAdmin admin, HTableDescriptor tableDescriptor) throws IOException;

  /**
   * Returns a list of {@link HRegionInfo} for the specified {@link TableId}
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId {@link TableId} for the specified table
   * @return a list of {@link HRegionInfo} for the specified {@link TableId}
   * @throws IOException
   */
  public abstract List<HRegionInfo> getTableRegions(HBaseAdmin admin, TableId tableId) throws IOException;

  /**
   * Deletes all tables in the specified namespace that satisfy the given {@link Predicate}.
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param namespaceId namespace for which the tables are being deleted
   * @param predicate The {@link Predicate} to decide whether to drop a table or not
   * @throws IOException
   */
  public void deleteAllInNamespace(HBaseAdmin admin, String namespaceId,
                                   Predicate<TableId> predicate) throws IOException {
    for (TableId tableId : listTablesInNamespace(admin, namespaceId)) {
      if (predicate.apply(tableId)) {
        dropTable(admin, tableId);
      }
    }
  }

  /**
   * Deletes all tables in the specified namespace
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param namespaceId namespace for which the tables are being deleted
   * @throws IOException
   */
  public void deleteAllInNamespace(HBaseAdmin admin, String namespaceId) throws IOException {
    deleteAllInNamespace(admin, namespaceId, Predicates.<TableId>alwaysTrue());
  }

  /**
   * Lists all tables in the specified namespace
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param namespaceId HBase namespace for which the tables are being requested
   */
  public abstract List<TableId> listTablesInNamespace(HBaseAdmin admin, String namespaceId) throws IOException;

  /**
   * Lists all tables
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   */
  public abstract List<TableId> listTables(HBaseAdmin admin) throws IOException;

  /**
   * Disables and deletes a table.
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId  {@link TableId} for the specified table
   * @throws IOException
   */
  public void dropTable(HBaseAdmin admin, TableId tableId) throws IOException {
    disableTable(admin, tableId);
    deleteTable(admin, tableId);
  }

  /**
   * Truncates a table
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId  {@link TableId} for the specified table
   * @throws IOException
   */
  public void truncateTable(HBaseAdmin admin, TableId tableId) throws IOException {
    HTableDescriptor tableDescriptor = getHTableDescriptor(admin, tableId);
    dropTable(admin, tableId);
    createTableIfNotExists(admin, tableId, tableDescriptor);
  }

  /**
   * Creates a {@link ScanBuilder}.
   */
  public ScanBuilder buildScan() {
    return new DefaultScanBuilder();
  }

  /**
   * Creates a {@link ScanBuilder} by copying from another {@link Scan} instance.
   */
  public ScanBuilder buildScan(Scan scan) throws IOException {
    return new DefaultScanBuilder(scan);
  }

  /**
   * Creates a {@link PutBuilder} for the given row.
   */
  public PutBuilder buildPut(byte[] row) {
    return new DefaultPutBuilder(row);
  }

  /**
   * Creates a {@link IncrementBuilder} for the given row.
   */
  public IncrementBuilder buildIncrement(byte[] row) {
    return new DefaultIncrementBuilder(row);
  }

  /**
   * Creates a {@link PutBuilder} by copying from another {@link Put} instance.
   */
  public PutBuilder buildPut(Put put) {
    return new DefaultPutBuilder(put);
  }

  /**
   * Creates a {@link GetBuilder} for the given row.
   */
  public GetBuilder buildGet(byte[] row) {
    return new DefaultGetBuilder(row);
  }

  /**
   * Creates a {@link GetBuilder} by copying from another {@link Get} instance.
   */
  public GetBuilder buildGet(Get get) {
    return new DefaultGetBuilder(get);
  }

  /**
   * Creates a {@link DeleteBuilder} for the given row.
   */
  public DeleteBuilder buildDelete(byte[] row) {
    return new DefaultDeleteBuilder(row);
  }

  /**
   * Creates a {@link DeleteBuilder} by copying from another {@link Delete} instance.
   */
  public DeleteBuilder buildDelete(Delete delete) {
    return new DefaultDeleteBuilder(delete);
  }

  public abstract void setCompression(HColumnDescriptor columnDescriptor, CompressionType type);

  public abstract void setBloomFilter(HColumnDescriptor columnDescriptor, BloomType type);

  public abstract CompressionType getCompression(HColumnDescriptor columnDescriptor);

  public abstract BloomType getBloomFilter(HColumnDescriptor columnDescriptor);

  public abstract Class<? extends Coprocessor> getTransactionDataJanitorClassForVersion();
  public abstract Class<? extends Coprocessor> getQueueRegionObserverClassForVersion();
  public abstract Class<? extends Coprocessor> getDequeueScanObserverClassForVersion();
  public abstract Class<? extends Coprocessor> getIncrementHandlerClassForVersion();
  public abstract Class<? extends Coprocessor> getMessageTableRegionObserverClassForVersion();
  public abstract Class<? extends Coprocessor> getPayloadTableRegionObserverClassForVersion();

  protected abstract HTableNameConverter getHTableNameConverter();

  /**
   * Collects HBase table stats
   * //TODO: Explore the possiblitity of returning a {@code Map<TableId, TableStats>}
   * @param admin instance of {@link HBaseAdmin} to communicate with HBase
   * @return map of table name -> table stats
   * @throws IOException
   */
  public Map<TableId, TableStats> getTableStats(HBaseAdmin admin) throws IOException {
    // The idea is to walk thru live region servers, collect table region stats and aggregate them towards table total
    // metrics.
    Map<TableId, TableStats> datasetStat = Maps.newHashMap();
    ClusterStatus clusterStatus = admin.getClusterStatus();

    HTableNameConverter hTableNameConverter = getHTableNameConverter();
    for (ServerName serverName : clusterStatus.getServers()) {
      Map<byte[], RegionLoad> regionsLoad = clusterStatus.getLoad(serverName).getRegionsLoad();

      for (RegionLoad regionLoad : regionsLoad.values()) {
        TableName tableName = HRegionInfo.getTable(regionLoad.getName());
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        if (!isCDAPTable(tableDescriptor)) {
          continue;
        }
        TableId tableId = hTableNameConverter.from(tableDescriptor);
        TableStats stat = datasetStat.get(tableId);
        if (stat == null) {
          stat = new TableStats(regionLoad.getStorefileSizeMB(), regionLoad.getMemStoreSizeMB());
          datasetStat.put(tableId, stat);
        } else {
          stat.incStoreFileSizeMB(regionLoad.getStorefileSizeMB());
          stat.incMemStoreSizeMB(regionLoad.getMemStoreSizeMB());
        }
      }
    }
    return datasetStat;
  }

  /**
   * Carries information about table stats
   */
  public static final class TableStats {
    private int storeFileSizeMB = 0;
    private int memStoreSizeMB = 0;

    public TableStats(int storeFileSizeMB, int memStoreSizeMB) {
      this.storeFileSizeMB = storeFileSizeMB;
      this.memStoreSizeMB = memStoreSizeMB;
    }

    public int getStoreFileSizeMB() {
      return storeFileSizeMB;
    }

    public int getMemStoreSizeMB() {
      return memStoreSizeMB;
    }

    void incStoreFileSizeMB(int deltaMB) {
      this.storeFileSizeMB += deltaMB;
    }

    void incMemStoreSizeMB(int deltaMB) {
      this.memStoreSizeMB += deltaMB;
    }

    public int getTotalSizeMB() {
      // both memstore and size on fs contribute to size of the dataset, otherwise user will be confused with zeroes
      // in dataset size even after something was written...
      return storeFileSizeMB + memStoreSizeMB;
    }
  }

  /**
   * Carries information about coprocessor information.
   */
  public static final class CoprocessorInfo {
    private final String className;
    private final Path path;
    private final int priority;
    private final Map<String, String> properties;

    private CoprocessorInfo(String className, Path path, int priority, Map<String, String> properties) {
      this.className = className;
      this.path = path;
      this.priority = priority;
      this.properties = ImmutableMap.copyOf(properties);
    }

    public String getClassName() {
      return className;
    }

    public Path getPath() {
      return path;
    }

    public int getPriority() {
      return priority;
    }

    public Map<String, String> getProperties() {
      return properties;
    }
  }
}
