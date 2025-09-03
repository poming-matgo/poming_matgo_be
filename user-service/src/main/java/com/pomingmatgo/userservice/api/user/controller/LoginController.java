package com.pomingmatgo.userservice.api.user.controller;

import com.pomingmatgo.userservice.api.user.request.LoginInfo;
import com.pomingmatgo.userservice.domain.user.service.LoginService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

//Auth Service에서 호출하는 Controller
@RestController
@RequiredArgsConstructor
@RequestMapping("/login")
public class LoginController {
    private final LoginService loginService;
    @GetMapping
    public boolean isLogin(@RequestBody LoginInfo loginInfo) {
        loginService.isLogin(loginInfo);
        return true;
    }
}
