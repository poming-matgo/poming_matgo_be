package com.pomingmatgo.gameservice.api.handler.event;


import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RequestEvent<T> {
    private EventType eventType;
    private T data;

    @JsonIgnore
    private long arrivalTime;

    @SuppressWarnings("unchecked")
    public <U> RequestEvent<U> as() {
        return (RequestEvent<U>) this;
    }
}
