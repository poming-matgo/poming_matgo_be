package com.pomingmatgo.gameservice.api.handler.event;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RequestEvent<T> {
    private EventType eventType;
    private T data;

    public <U> RequestEvent<U> withData(U newData) {
        RequestEvent<U> newEvent = new RequestEvent<>();
        newEvent.setEventType(this.eventType);
        newEvent.setData(newData);
        return newEvent;
    }
}
