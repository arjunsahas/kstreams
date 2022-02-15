package org.arjun.generator;

import lombok.Getter;
import org.arjun.model.TimeModel;
import org.springframework.context.ApplicationEvent;

@Getter
public class TimeSeriesEvent extends ApplicationEvent {
    private final TimeModel model;

    public TimeSeriesEvent(Object source, TimeModel model) {
        super(source);
        this.model = model;
    }
}
