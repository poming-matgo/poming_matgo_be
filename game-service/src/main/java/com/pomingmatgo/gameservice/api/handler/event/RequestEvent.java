package com.pomingmatgo.gameservice.api.handler.event;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RequestEvent<T> {
    private EventType eventType;
    private int playerNum;
    private long roomId;
    private T data;

    public <U> RequestEvent<U> withData(U newData) {
        RequestEvent<U> newEvent = new RequestEvent<>();
        newEvent.setEventType(this.eventType);
        newEvent.setPlayerNum(this.playerNum);
        newEvent.setRoomId(this.roomId);
        newEvent.setData(newData);
        return newEvent;
    }
}
