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
        //todo: 아래 예외처리 구현 해야함
        /*if(isEmailDuplicate(req.getEmail())
            throw;
        if(isNicknameDuplicate(req.getNickname()))
            throw;*/
        
        //todo: 비밀번호 해싱 구현 해야함

        System.out.println("email: " + req.getEmail());
        System.out.println("nickname: " + req.getNickname());
        userRepository.save(userMapper.toUser(req));

    }

    //이메일 중복 검사
    public boolean isEmailDuplicate(String email) {
        return userRepository.existsByEmail(email);
    }

    //닉네임 중복 검사
    public boolean isNicknameDuplicate(String nickname) {
        return userRepository.existsByNickname(nickname);
    }
}
