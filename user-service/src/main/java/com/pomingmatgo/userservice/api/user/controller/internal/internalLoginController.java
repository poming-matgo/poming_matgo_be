package com.pomingmatgo.userservice.api.user.controller.internal;

import com.pomingmatgo.userservice.api.user.request.LoginInfo;
import com.pomingmatgo.userservice.domain.user.User;
import com.pomingmatgo.userservice.domain.user.service.LoginService;
import io.swagger.v3.oas.annotations.Hidden;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

//Auth Service에서 호출하는 Controller
@RestController
@RequiredArgsConstructor
@Hidden
@RequestMapping("/login-process")
public class internalLoginController {
    private final LoginService loginService;
    @GetMapping
    public User isLogin(LoginInfo loginInfo) {
        return loginService.isLogin(loginInfo).orElse(null);
    }
}
