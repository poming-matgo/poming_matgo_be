package com.pomingmatgo.gameservice.api.handler.event;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RequestEvent<T> {
    private EventType eventType;
    private T data;

    // 역직렬화(GameWebSocketHandler.handleMessage) 시 한 번 확정 — 이후 핸들러들은 문자열 재파싱 없이 공유
    @JsonIgnore
    private SubCategory subCategory;

    @SuppressWarnings("unchecked")
    public <U> RequestEvent<U> as() {
        return (RequestEvent<U>) this;
    }
}
