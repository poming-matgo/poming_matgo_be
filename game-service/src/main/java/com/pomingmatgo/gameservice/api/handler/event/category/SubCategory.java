package com.pomingmatgo.gameservice.api.handler.event.category;

import com.pomingmatgo.gameservice.api.request.websocket.GoStopReq;
import com.pomingmatgo.gameservice.api.request.websocket.JoinRoomReq;
import com.pomingmatgo.gameservice.api.request.websocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import lombok.Getter;

@Getter
public enum SubCategory {
    CONNECT(EventCategory.ROOM, JoinRoomReq.class),
    READY(EventCategory.ROOM, Void.class), // payload가 없는 경우
    UNREADY(EventCategory.ROOM, Void.class),
    LEADER_SELECTION(EventCategory.PREGAME, LeadSelectionReq.class),
    NORMAL_SUBMIT(EventCategory.GAME, NormalSubmitReq.class),
    FLOOR_SELECT(EventCategory.GAME, NormalSubmitReq.class),
    GO_STOP_CHOICE(EventCategory.GAME, GoStopReq.class);

    private final EventCategory category;
    private final Class<?> payloadClass;

    SubCategory(EventCategory category, Class<?> payloadClass) {
        this.category = category;
        this.payloadClass = payloadClass;
    }

    public static SubCategory from(String value) {
        try {
            return SubCategory.valueOf(value);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IllegalArgumentException("Unsupported SubType: " + value);
        }
    }
}
