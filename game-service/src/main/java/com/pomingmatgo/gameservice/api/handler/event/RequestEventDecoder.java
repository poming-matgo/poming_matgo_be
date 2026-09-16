package com.pomingmatgo.gameservice.api.handler.event;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pomingmatgo.gameservice.api.handler.event.category.SubCategory;
import com.pomingmatgo.gameservice.global.exception.WebSocketBusinessException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import static com.pomingmatgo.gameservice.global.exception.WebSocketErrorCode.INVALID_REQUEST;

@Component
@RequiredArgsConstructor
public class RequestEventDecoder {
    private final ObjectMapper objectMapper;

    public Mono<RequestEvent<?>> decode(String payload) {
        return Mono.<RequestEvent<?>>fromCallable(() -> {
                    JsonNode rootNode = objectMapper.readTree(payload);
                    String subTypeStr = rootNode.path("eventType").path("subType").asText();

                    SubCategory subType = SubCategory.from(subTypeStr);
                    JavaType type = objectMapper.getTypeFactory().constructParametricType(RequestEvent.class, subType.getPayloadClass());

                    RequestEvent<?> event = objectMapper.convertValue(rootNode, type);
                    event.setSubCategory(subType);
                    if (subType.getPayloadClass() != Void.class && event.getData() == null) {
                        throw new WebSocketBusinessException(INVALID_REQUEST);
                    }
                    return event;
                })
                .onErrorMap(e -> !(e instanceof WebSocketBusinessException),
                        e -> new WebSocketBusinessException(INVALID_REQUEST));
    }
}
