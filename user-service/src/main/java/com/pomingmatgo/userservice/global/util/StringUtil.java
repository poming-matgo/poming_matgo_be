package com.pomingmatgo.userservice.global.util;

import java.security.SecureRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class StringUtil {
    private StringUtil() {}
    public static String generateString(int length) {
        SecureRandom secureRandom = new SecureRandom();
        String charList =
                IntStream.rangeClosed(33, 126)
                        .mapToObj(i -> String.valueOf((char) i))
                        .collect(Collectors.joining());

        return secureRandom.ints(length, 0, charList.length())
                .mapToObj(charList::charAt)
                .map(String::valueOf)
                .collect(Collectors.joining());
    }
}
