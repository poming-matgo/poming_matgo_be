package com.pomingmatgo.authservice.domain.service.login;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class LoginStrategyFactory {
    private final Map<String, LoginStrategy> strategyMap;

    @Autowired
    public LoginStrategyFactory(List<LoginStrategy> strategies) {
        this.strategyMap = strategies.stream()
                .collect(Collectors.toMap(strategy -> strategy.resolveLoginType().name().toLowerCase(), strategy -> strategy));
    }

    public LoginStrategy getStrategy(String registrationId) {
        return Optional.ofNullable(strategyMap.get(registrationId))
                .orElseThrow(() -> new IllegalArgumentException("지원하지 않는 소셜 로그인입니다."));
    }
}