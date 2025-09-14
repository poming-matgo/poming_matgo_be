package com.pomingmatgo.gameservice.api.handler.event;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RequestEvent<T> {
    private EventType eventType;
    private long userId;
    private long roomId;
    private T data;
}
