package com.pomingmatgo.userservice.domain.user.service;

import com.pomingmatgo.userservice.api.user.request.LoginInfo;
import com.pomingmatgo.userservice.domain.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LoginService {
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    public boolean isLogin(LoginInfo loginInfo) {
        return userRepository.findByEmail(loginInfo.getEmail())
                .map(user -> user.getPassword().equals(passwordEncoder.encode(loginInfo.getPassword())))
                .orElse(false);
    }
}
