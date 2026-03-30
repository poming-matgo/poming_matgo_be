package com.pomingmatgo.gameservice.global.netty;

import io.netty.util.AttributeKey;

public class NettyAttributes {
    public static final AttributeKey<Long> PACKET_ARRIVAL_TIME = AttributeKey.valueOf("packetArrivalTime");
}