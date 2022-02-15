package org.arjun.generator;

import lombok.extern.slf4j.Slf4j;
import org.arjun.model.TimeModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class TimeSeriesGenerator {


    public TimeSeriesGenerator() {
    }

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @EventListener(ApplicationStartedEvent.class)
    public void emitNumber() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new TimeSeriesRunnable());
    }

    @PostConstruct
    public void init() {
    }

    class TimeSeriesRunnable implements Runnable {

        @Override
        public void run() {
            for (int i = 0; i < 1000; i++) {
                Date dateTime = new Date(System.currentTimeMillis());
                TimeModel model = TimeModel.builder().dateTime(dateTime).name("T" + dateTime.getTime()).number(i).build();
                eventPublisher.publishEvent(new TimeSeriesEvent(this, model));
            }
        }
    }
}
