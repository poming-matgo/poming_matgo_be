package com.pomingmatgo.gameservice.global.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ArrivalTimeRecordHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        long arrivalTime = System.currentTimeMillis();
        ctx.channel().attr(NettyAttributes.PACKET_ARRIVAL_TIME).set(arrivalTime);
        super.channelRead(ctx, msg);
    }
}