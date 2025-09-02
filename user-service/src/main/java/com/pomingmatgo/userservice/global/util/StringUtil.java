package com.pomingmatgo.userservice.global.util;

import java.security.SecureRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class StringUtil {
    private StringUtil() {}
    public static String generateString(int length) {
        SecureRandom secureRandom = new SecureRandom();
        // 숫자(0-9)와 대문자(A-Z), 소문자(a-z)로 제한
        String charList = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        return secureRandom.ints(length, 0, charList.length())
                .mapToObj(charList::charAt)
                .map(String::valueOf)
                .collect(Collectors.joining());
    }
}
