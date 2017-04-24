package home;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


public class LauncherMain {
  private static final Logger logger = LoggerFactory.getLogger(LauncherMain.class);

  private static final String TEST_ADDRESS = "test.channel";
  private static final String LOCK1 = "lock1";
  private static final String LOCK2 = "lock2";

  public static void main(String[] args) throws InterruptedException {
    boolean clustered = true;
    Vertx vertx = createVertxSystem(clustered);
    vertx.deployVerticle(new TestVerticle(), new DeploymentOptions());
    vertx.setPeriodic(1, i -> vertx.eventBus().send(TEST_ADDRESS, "msg"));
  }

  private static final class TestVerticle extends AbstractVerticle {
    @Override
    public void start() throws Exception {
      vertx.eventBus().consumer(TEST_ADDRESS, event -> {
        vertx.sharedData().getLock(LOCK1, res -> {
          if (res.succeeded()) {
            Lock lock1 = res.result();
            vertx.sharedData().getLock(LOCK2, res2 -> {
              if (res2.succeeded()) {
                Lock lock2 = res2.result();
                logger.info("All locks catched");
                lock2.release();
              }
              lock1.release();
            });
          }
        });
      });
    }
  }

  private static Vertx createVertxSystem(boolean clustered) throws InterruptedException {
    final Vertx vertx;
    if (clustered) {
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Vertx> vertexRef = new AtomicReference<>(null);
      createClusteredSystem(latch, vertexRef);
      if (!latch.await(1, TimeUnit.MINUTES) || vertexRef.get() == null) {
        throw new IllegalStateException("Vertx system isn't started");
      }
      vertx = vertexRef.get();
    } else {
      vertx = Vertx.factory.vertx();
    }
    return vertx;
  }

  private static void createClusteredSystem(CountDownLatch latch, AtomicReference<Vertx> vertexRef) {
    VertxOptions options = new VertxOptions().setClusterManager(new HazelcastClusterManager());
    Vertx.factory.clusteredVertx(options, res -> {
      if (!res.succeeded()) {
        throw new IllegalStateException("Can't create clustered vertx system", res.cause());
      } else {
        latch.countDown();
        vertexRef.set(res.result());
      }
    });
  }

}
