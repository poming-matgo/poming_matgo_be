package com.pomingmatgo.userservice.api.user.controller;

import com.pomingmatgo.userservice.api.user.request.LoginInfo;
import com.pomingmatgo.userservice.domain.user.User;
import com.pomingmatgo.userservice.domain.user.service.LoginService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

//Auth Service에서 호출하는 Controller
@RestController
@RequiredArgsConstructor
@RequestMapping("/login-process")
public class LoginController {
    private final LoginService loginService;
    @GetMapping
    public User isLogin(LoginInfo loginInfo) {
        User user = loginService.isLogin(loginInfo).orElse(null);
        return user;
    }
}
