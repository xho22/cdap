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
import sun.security.x509.CertAndKeyGen;
import sun.security.x509.X500Name;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

/**
 * File based keystore for use with SSL.
 */
public class GeneratedCertKeyStoreCreator {

  public static KeyStore getSSLKeyStore(SConfiguration sConf) {

    KeyStore ks;
    try {
      CertAndKeyGen keyGen = new CertAndKeyGen("RSA", "SHA1WithRSA", null);
      keyGen.generate(1024);
      String password = "pass";
      //Generate self signed certificate
      X509Certificate[] chain = new X509Certificate[1];
      chain[0] = keyGen.getSelfCertificate(new X500Name("CN=ROOT"), (long) 365 * 24 * 3600);
      InputStream inputStream = new ByteArrayInputStream(chain[0].getEncoded());

      ks = KeyStore.getInstance(sConf.get(Constants.Security.AppFabric.SSL_KEYSTORE_TYPE));
      ks.load(inputStream, password.toCharArray());
    } catch (Throwable e) {
      throw new RuntimeException("SSL is enabled but the keystore file could not be read. Please verify that the " +
                                   "keystore file exists and the path is set correctly : "
                                   + sConf.get(Constants.Security.AppFabric.SSL_FILE_KEYSTORE_PATH));
    }
    return ks;
  }
}
