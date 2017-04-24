package home;

import io.vertx.core.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;


public class LauncherMain {
  private static final Logger logger = LoggerFactory.getLogger(LauncherMain.class);

  private static final String TEST_ADDRESS = "test.channel";
  private static final String LOCK1 = "lock1";
  private static final String LOCK2 = "lock2";

  public static void main(String[] args) throws InterruptedException {
    final boolean clustered = true;
    final Vertx vertx = createVertxSystem(clustered);
    final TestVerticle verticle = new TestVerticle();
    vertx.deployVerticle(verticle, new DeploymentOptions());

    final LongAdder sendCount = new LongAdder();
    logger.info("Start sending");
    vertx.setPeriodic(1, i -> {
      vertx.eventBus().send(TEST_ADDRESS, "msg");
      sendCount.increment();
    });

    vertx.setPeriodic(TimeUnit.SECONDS.toMillis(5), i -> {
      logger.info("Sent " + sendCount.longValue() + " messages; consumed with locks: " + verticle.getConsumedWithLocks());
    });
  }

  private static final class TestVerticle extends AbstractVerticle {
    private final LongAdder consumedWithLocks = new LongAdder();

    @Override
    public void start() throws Exception {
      vertx.eventBus().consumer(TEST_ADDRESS, event -> vertx.sharedData().getLock(LOCK1, result -> {
        if (result.succeeded()) {
          getSecondLock(result.result());
        }
      }));
    }

    private void getSecondLock(Lock lock) {
      vertx.sharedData().getLock(LOCK2, result -> {
        withSecondLock(result, this::workWithLocks);
        lock.release();
      });
    }

    private void withSecondLock(AsyncResult<Lock> result, Runnable with2Locks) {
      if (result.succeeded()) {
        final Lock lock = result.result();
        with2Locks.run();
        lock.release();
      }
    }

    private void workWithLocks() {
      logger.info("Consumed with all locks");
      consumedWithLocks.increment();
    }

    private Long getConsumedWithLocks() {
      return consumedWithLocks.longValue();
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
    final VertxOptions options = new VertxOptions().setClusterManager(new HazelcastClusterManager());
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
