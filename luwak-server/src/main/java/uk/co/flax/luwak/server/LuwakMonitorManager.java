package uk.co.flax.luwak.server;

import io.dropwizard.lifecycle.Managed;
import uk.co.flax.luwak.Monitor;

/**
 * Created by Tom.Ridd on 20/01/2017.
 */
public class LuwakMonitorManager implements Managed {
  final Monitor monitor;

  public LuwakMonitorManager(Monitor monitor) {
    this.monitor = monitor;
  }

  @Override
  public void start() throws Exception {
    // Monitor manager started on monitor creation
  }

  @Override
  public void stop() throws Exception {
    monitor.close();
  }
}
