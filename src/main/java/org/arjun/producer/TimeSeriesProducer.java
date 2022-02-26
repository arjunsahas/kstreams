package org.arjun.producer;


import org.arjun.generator.TimeSeriesEvent;
import org.arjun.model.TimeModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static org.arjun.config.Config.TM_TOPIC;

@Component
public class TimeSeriesProducer {

    @Autowired
    private KafkaTemplate<String, Long> timeModelKafkaTemplate;

    @EventListener(TimeSeriesEvent.class)
    public void produce(TimeSeriesEvent event) {
        TimeModel model = event.getModel();
        System.out.println("Producer: " + model);
        System.out.println("Producer: " + model.getDateTime().getTime());
        timeModelKafkaTemplate.send(TM_TOPIC, model.getName(), model.getDateTime().getTime());
    }
}
