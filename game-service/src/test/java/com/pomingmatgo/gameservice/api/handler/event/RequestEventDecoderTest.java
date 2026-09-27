package com.pomingmatgo.gameservice.api.handler.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.api.handler.event.RequestEventDecoder;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.api.request.websocket.NormalSubmitReq;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_REQUEST;
import static org.junit.jupiter.api.Assertions.*;

class RequestEventDecoderTest {
    private final RequestEventDecoder decoder = new RequestEventDecoder(new ObjectMapper());

    @ParameterizedTest
    @ValueSource(strings = {
            "not json", "", "null", "{}",
            "{\"eventType\":{\"subType\":\"UNKNOWN\"}}",
            "{\"eventType\":{\"subType\":\"NORMAL_SUBMIT\"}}",
            "{\"eventType\":{\"subType\":\"NORMAL_SUBMIT\"},\"data\":{\"cardIndex\":\"invalid\"}}"
    })
    void rejectsInvalidRequests(String payload) {
        StepVerifier.create(decoder.decode(payload))
                .expectErrorSatisfies(error -> {
                    var businessError = assertInstanceOf(WebSocketBusinessException.class, error);
                    assertEquals(INVALID_REQUEST, businessError.getWebsocketErrorCode());
                })
                .verify();
    }

    @Test
    void decodesTypedPayloadUsingSubCategory() {
        StepVerifier.create(decoder.decode("""
                {"eventType":{"type":"ROOM","subType":"NORMAL_SUBMIT"},"data":{"cardIndex":2}}
                """))
                .assertNext(event -> {
                    assertEquals(SubCategory.NORMAL_SUBMIT, event.getSubCategory());
                    assertEquals(2, assertInstanceOf(NormalSubmitReq.class, event.getData()).cardIndex());
                })
                .verifyComplete();
    }

    @Test
    void acceptsReadyWithoutPayload() {
        StepVerifier.create(decoder.decode("""
                {"eventType":{"subType":"READY"}}
                """))
                .assertNext(event -> {
                    assertEquals(SubCategory.READY, event.getSubCategory());
                    assertNull(event.getData());
                })
                .verifyComplete();
    }
}
