package com.pomingmatgo.gameservice.global;

// WS REDIRECT data / HTTP 421 body 공용 — target은 광고 주소(host:port) 그대로, 스킴·경로 조립은 클라 몫
public record RedirectRes(String target) {
}
