package com.pomingmatgo.userservice.domain.user.service;

import com.pomingmatgo.userservice.api.user.request.RegisterRequest;
import com.pomingmatgo.userservice.domain.user.mapper.UserMapper;
import com.pomingmatgo.userservice.domain.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class RegisterService {
    private final UserRepository userRepository;
    private final UserMapper userMapper;
    public void register(RegisterRequest req) {
        if(isEmailDuplicate(req.getEmail())
            throw;
        if(isNicknameDuplicate(req.getNickname()))
            throw;

        userRepository.save(userMapper.toUser(req));

    }

    //이메일 중복 검사
    public boolean isEmailDuplicate(String email) {
        return userRepository.existsByEmail(email);
    }

    //닉네임 중복 검사
    public boolean isNicknameDuplicate(String nickname) {
        return userRepository.existsByNickName(nickname);
    }
}
