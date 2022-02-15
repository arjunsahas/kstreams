package org.arjun.model;

import lombok.*;

import java.util.Date;

@Getter
@Setter
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class TimeModel {
    private String name;
    private Date dateTime;
    private int number;
}

