package com.pomingmatgo.gameservice.api.handler.event;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RequestEvent<T> {
    private EventType eventType;
    private T data;

    @SuppressWarnings("unchecked")
    public <U> RequestEvent<U> as() {
        return (RequestEvent<U>) this;
    }
}
