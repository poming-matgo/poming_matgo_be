package com.pomingmatgo.authservice.global.config;

import com.pomingmatgo.authservice.domain.User;
import lombok.RequiredArgsConstructor;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

@Service
@RequiredArgsConstructor
public class TokenService {
    private final JwtEncoder jwtEncoder;

    public String generateAccessToken(User user) {
        Instant now = Instant.now();
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer("pomingMatgo") // 토큰 발급자
                .issuedAt(now)
                .expiresAt(now.plus(5, ChronoUnit.MINUTES))
                .subject(String.valueOf(user.getId()))
                .claims(createClaims(user))
                .build();

        return jwtEncoder.encode(JwtEncoderParameters.from(claims)).getTokenValue();
    }

    public String generateRefreshToken(User user) {
        Instant now = Instant.now();
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer("pomingMatgo") // 토큰 발급자
                .issuedAt(now)
                .expiresAt(now.plus(30, ChronoUnit.DAYS))
                .subject(String.valueOf(user.getId()))
                .build();

        return jwtEncoder.encode(JwtEncoderParameters.from(claims)).getTokenValue();
    }


    private Consumer<Map<String, Object>> createClaims(User user) {
        return claims -> {
            //claims.put("identifier", user.getIdentifier());
            claims.put("role", Collections.emptySet());
        };
    }
}