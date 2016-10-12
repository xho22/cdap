package co.cask.cdap.security.tools;

import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;

public class KMSCertificateFetcher implements SSLCertificateFetcher {

  private final KeyProvider provider;
  private final Configuration conf;

  @Inject
  public KMSCertificateFetcher(Configuration conf) {
    this.conf = conf;
  }
}
