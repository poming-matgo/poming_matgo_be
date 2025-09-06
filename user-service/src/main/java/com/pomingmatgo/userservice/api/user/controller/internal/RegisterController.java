package com.pomingmatgo.userservice.api.user.controller.internal;

import com.pomingmatgo.userservice.api.user.request.SocialRegisterRequest;
import com.pomingmatgo.userservice.domain.user.User;
import com.pomingmatgo.userservice.domain.user.service.RegisterService;
import io.swagger.v3.oas.annotations.Hidden;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/register")
@RestController
@RequiredArgsConstructor
@Hidden
public class RegisterController {
    private final RegisterService registerService;

    @PostMapping("/social-signup")
    public User oauth2Signup(@RequestBody SocialRegisterRequest req) {
        return registerService.socialRegister(req);
    }
}
