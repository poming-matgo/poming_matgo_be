package com.pomingmatgo.userservice.domain.user.service;

import com.pomingmatgo.userservice.api.user.request.LoginInfo;
import com.pomingmatgo.userservice.domain.user.AuthUser;
import com.pomingmatgo.userservice.domain.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class LoginService {
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    public Optional<AuthUser> isLogin(LoginInfo loginInfo) {
        return userRepository.findByIdentifier(loginInfo.getIdentifier())
                .filter(user -> passwordEncoder.matches(loginInfo.getPassword(), user.getPassword()))
                .map(user -> new AuthUser(user.getId(), user.getIdentifier()));
    }

}
