package io.github.laiwf94.keel.server;

import java.io.IOException;
import java.nio.file.Path;

/** Lets tests outside this package start a local cluster. */
public final class ClusterAccess implements AutoCloseable {
  private final LocalCluster cluster;

  public ClusterAccess(Path root, int n) throws IOException {
    this.cluster = new LocalCluster(root, n);
  }

  public ClusterAccess tokens(String client, String admin) {
    cluster.clientToken = java.util.Optional.ofNullable(client);
    cluster.adminToken = java.util.Optional.ofNullable(admin);
    return this;
  }

  public ClusterAccess startAll() throws IOException {
    cluster.startAll();
    return this;
  }

  public String url(String id) {
    return cluster.url(id);
  }

  public String endpoints() {
    return String.join(",", cluster.ports.keySet().stream().map(cluster::url).toList());
  }

  public String leaderId() throws InterruptedException {
    return cluster.leaderId();
  }

  public void stop(String id) {
    cluster.stop(id);
  }

  public void start(String id) throws IOException {
    cluster.start(id);
  }

  public java.util.Set<String> ids() {
    return cluster.ports.keySet();
  }

  @Override
  public void close() {
    cluster.close();
  }
}
