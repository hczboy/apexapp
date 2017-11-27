package com.polycom.analytic.tranquility;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.FutureEventListener;

import scala.runtime.BoxedUnit;

public class TranquilitySender implements Closeable
{
    private static final Logger log = LoggerFactory.getLogger(TranquilitySender.class);
    private ExecutorService executorService;
    private ArrayBlockingQueue<String> pendingEventQueue;

    private AtomicBoolean isAlive = new AtomicBoolean(false);

    private TranquilityOutputOperator ownerOperator = null;

    private Tranquilizer<Map<String, Object>> sender;

    public TranquilitySender()
    {

    }

    public void putEvent(String event)
    {
        try
        {
            pendingEventQueue.put(event);
        }
        catch (InterruptedException e)
        {
            // TODO implement catch InterruptedException
            log.error("got interrupted", e);
            throw new IllegalStateException("interrupted unexpectedly when putEvent", e);

        }
    }

    public void start()
    {
        final InputStream configStream = TranquilityOutputOperator.class.getClassLoader()
                .getResourceAsStream("server.json");
        final TranquilityConfig<PropertiesBasedConfig> config = TranquilityConfig.read(configStream);
        final DataSourceConfig<PropertiesBasedConfig> deviceEventConfig = config.getDataSource("deviceEvent");
        sender = DruidBeams.fromConfig(deviceEventConfig)
                .buildTranquilizer(deviceEventConfig.tranquilizerBuilder());
        sender.start();

        executorService = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("tranquilitySender-thread-%d").build());
        for (int i = 0; i < ownerOperator.getSenderThreadCount(); i++)
        {
            executorService.submit(new SenderThread());
        }

        isAlive.set(true);
    }

    public void stop()
    {
        isAlive.set(false);
        sender.flush();
        sender.stop();
        executorService.shutdownNow();
    }

    public void create(TranquilityOutputOperator ownerOperator)
    {
        this.ownerOperator = ownerOperator;

        if (Objects.isNull(ownerOperator.getPendingEventQueue()))
        {
            log.info("pendingEventQueue is null in application[{}].operator[{}], create one",
                    ownerOperator.getAppName(), ownerOperator.getOperatorId());

            ownerOperator.setPendingEventQueue(
                    new ArrayBlockingQueue<String>(ownerOperator.getPendingEventQueueSize()));
        }
        pendingEventQueue = ownerOperator.getPendingEventQueue();
    }

    @Override
    public void close() throws IOException
    {

    }

    final class SenderThread implements Runnable
    {

        @Override
        public void run()
        {
            String eventStr = null;
            while (isAlive.get())
            {
                try
                {

                    eventStr = pendingEventQueue.take();

                }
                catch (InterruptedException e)
                {

                    log.error("Unexpected Exception", e);
                    throw new IllegalStateException("unexpeced interrput when take event from queue", e);

                }
                final Map<String, Object> event = JSON.parseObject(eventStr);
                sender.send(event).addEventListener(new FutureEventListener<BoxedUnit>()
                {

                    @Override
                    public void onFailure(Throwable t)
                    {
                        if (t instanceof MessageDroppedException)
                        {
                            log.warn("Dropped event: {}", event, t);

                        }
                        else
                        {
                            log.error("Failed to send message: {}", event, t);

                        }

                    }

                    @Override
                    public void onSuccess(BoxedUnit arg0)
                    {
                        log.debug("success send event: {}", event);

                    }
                });

            }
        }

    }

}
