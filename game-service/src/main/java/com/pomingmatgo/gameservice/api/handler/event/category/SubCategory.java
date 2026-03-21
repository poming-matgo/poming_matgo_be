package com.pomingmatgo.gameservice.api.handler.event.category;

import com.pomingmatgo.gameservice.api.request.websocket.GoStopReq;
import com.pomingmatgo.gameservice.api.request.websocket.JoinRoomReq;
import com.pomingmatgo.gameservice.api.request.websocket.LeadSelectionReq;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import lombok.Getter;

@Getter
public enum SubCategory {
    CONNECT(JoinRoomReq.class),
    READY(Void.class), // payload가 없는 경우
    UNREADY(Void.class),
    LEADER_SELECTION(LeadSelectionReq.class),
    NORMAL_SUBMIT(NormalSubmitReq.class),
    FLOOR_SELECT(NormalSubmitReq.class),
    GO_STOP_CHOICE(GoStopReq.class);

    private final Class<?> payloadClass;

    SubCategory(Class<?> payloadClass) {
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