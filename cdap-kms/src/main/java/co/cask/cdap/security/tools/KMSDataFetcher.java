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

package co.cask.cdap.security.tools;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 * KMS based implementation. Fetches the private key data from Hadoop KMS.
 */
public class KMSDataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(KMSDataFetcher.class);

  private static KeyProvider provider;

  @Inject
  public KMSDataFetcher(Configuration conf) throws IOException, URISyntaxException {
    String keyProviderPath = conf.get(KeyProviderFactory.KEY_PROVIDER_PATH);
    if (Strings.isNullOrEmpty(keyProviderPath)) {
      throw new IllegalArgumentException("Could not find the key provider URI. Please make sure that " +
                                           "hadoop.security.key.provider.path is set to the KMS URI in your " +
                                           "core-site.xml.");
    }
    URI providerUri = new URI(keyProviderPath);
    provider = KMSClientProvider.Factory.get(providerUri, conf);
    LOG.debug("Key provider initialized successfully.");
  }

  public KeyStore getSSLKeyStore(SConfiguration sConf) {
    KeyStore ks = null;
    String password = sConf.get(Constants.Security.AppFabric.SSL_KEYSTORE_PASSWORD);
    try {
      byte[] keyData = getKeyData(Constants.Security.AppFabric.SSL_CERT_KMS_KEYNAME);
      InputStream is = new ByteArrayInputStream(keyData);
      ks = KeyStore.getInstance("JCEKS");
      ks.load(is, password.toCharArray());
    } catch (KeyStoreException | CertificateException | IOException | NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    return ks;
  }

  private byte[] getKeyData(String key) throws IOException {
    KeyProvider.KeyVersion keyVersion = provider.getCurrentKey(key);
    return keyVersion.getMaterial();
  }
}
