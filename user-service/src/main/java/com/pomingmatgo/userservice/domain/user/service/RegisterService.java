package com.pomingmatgo.userservice.domain.user.service;

import com.pomingmatgo.userservice.api.user.request.RegisterRequest;
import com.pomingmatgo.userservice.domain.user.mapper.UserMapper;
import com.pomingmatgo.userservice.domain.user.mapper.UserTmpMapper;
import com.pomingmatgo.userservice.domain.user.repository.UserRepository;
import com.pomingmatgo.userservice.domain.user.repository.UserTmpRepository;
import com.pomingmatgo.userservice.global.exception.BusinessException;
import com.pomingmatgo.userservice.global.exception.ErrorCode;
import com.pomingmatgo.userservice.global.exception.SystemException;
import com.pomingmatgo.userservice.global.util.StringUtil;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.InternetAddress;
import lombok.RequiredArgsConstructor;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import jakarta.mail.internet.MimeMessage;

import java.io.UnsupportedEncodingException;

import static com.pomingmatgo.userservice.global.exception.ErrorCode.EMAIL_SEND_FAILED;
import static com.pomingmatgo.userservice.global.exception.ErrorCode.SYSTEM_ERROR;

@Service
@RequiredArgsConstructor
public class RegisterService {
    private final UserRepository userRepository;
    private final UserTmpMapper userTmpMapper;
    private final UserMapper userMapper;
    private final UserTmpRepository userTmpRepository;
    private final JavaMailSender mailSender;
    private final PasswordEncoder passwordEncoder;

    public void register(RegisterRequest req) {
        if(isEmailDuplicate(req.getEmail()))
            throw new BusinessException(ErrorCode.DUPLICATE_EMAIL);
        if(isNicknameDuplicate(req.getNickname()))
            throw new BusinessException(ErrorCode.DUPLICATE_NICKNAME);

        String encodedPassword = passwordEncoder.encode(req.getPassword());

        String randomString = StringUtil.generateString(20);
        sendAuthMessage(req.getEmail(), randomString);
        userTmpRepository.save(userTmpMapper.toUserTmp(req, randomString, encodedPassword)); //메일 인증 전까지는 redis에 임시 저장
    }

    private MimeMessage createMessage(String email, String randomString) {
        MimeMessage message = mailSender.createMimeMessage();
        String sendMsg = String.format("<p>아래 링크를 클릭하여 회원가입을 완료하시오. 이 링크는 10분간 유효합니다.<br>localhost:8082/user/register/auth?code=%s</p>", randomString);
        try {
            message.setRecipients(Message.RecipientType.TO, email);
            message.setSubject("포밍맞고 인증 메일 입니다.");
            message.setText(sendMsg, "utf-8", "html");
            message.setFrom(new InternetAddress("life_is_choice@naver.com", "poming"));
        }
        catch(MessagingException e) {
            throw new SystemException(EMAIL_SEND_FAILED);
        }
        catch(UnsupportedEncodingException e) {
            throw new SystemException(SYSTEM_ERROR);
        }

        return message;
    }
    public void sendAuthMessage(String email, String randomString) {
        MimeMessage message = createMessage(email, randomString);
        try {
            mailSender.send(message); //재시도 로직 추가할까? 아니다.. 사용자나 프론트가 재시도하게 하자
        } catch(MailException e) {
            throw new SystemException(EMAIL_SEND_FAILED);
        }

    }

    //이메일 중복 검사
    public boolean isEmailDuplicate(String email) {
        return userRepository.existsByEmail(email) || userTmpRepository.existsByEmail(email);
    }

    //닉네임 중복 검사
    public boolean isNicknameDuplicate(String nickname) {
        return userRepository.existsByNickname(nickname) || userTmpRepository.existsByNickname(nickname);
    }

    public boolean completeRegistration(String authId) {
        return userTmpRepository.findById(authId)
                .map(tmpUser -> {
                    userRepository.save(userMapper.toUser(tmpUser));
                    userTmpRepository.delete(tmpUser);
                    return true;
                })
                .orElse(false);
    }
}
