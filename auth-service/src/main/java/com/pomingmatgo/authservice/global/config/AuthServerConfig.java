package com.pomingmatgo.authservice.global.config;

import com.pomingmatgo.authservice.global.security.ExternalUserAuthenticationProvider;
import com.pomingmatgo.authservice.global.security.SocialLoginSuccessHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;
import org.springframework.security.oauth2.server.authorization.InMemoryOAuth2AuthorizationService;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationService;
import org.springframework.security.oauth2.server.authorization.client.InMemoryRegisteredClientRepository;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.security.oauth2.server.authorization.config.annotation.web.configuration.OAuth2AuthorizationServerConfiguration;
import org.springframework.security.oauth2.server.authorization.settings.AuthorizationServerSettings;
import org.springframework.security.oauth2.server.authorization.settings.ClientSettings;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.util.matcher.MediaTypeRequestMatcher;

import java.util.UUID;

import static org.springframework.security.config.Customizer.withDefaults;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class AuthServerConfig {

    private final AuthenticationProvider authenticationProvider;
    private final ClientRegistrationRepository clientRegistrationRepository;
    private final SocialLoginSuccessHandler socialLoginSuccessHandler;

    @Value("${oauth.react.clientId}")
    private String clientId;

    @Value("${oauth.react.clientSecret}")
    private String clientSecret;

    @Value("${oauth.react.redirectUri}")
    private String redirectUri;

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
    @Bean
    @Order(1)
    public SecurityFilterChain authorizationServerSecurityFilterChain(HttpSecurity http)
            throws Exception {
        OAuth2AuthorizationServerConfiguration.applyDefaultSecurity(http);
        http.exceptionHandling(exceptions -> exceptions.defaultAuthenticationEntryPointFor( // 인가 실패에 대한 처리를 정의
                new LoginUrlAuthenticationEntryPoint("/custom-login"),
                new MediaTypeRequestMatcher(MediaType.TEXT_HTML)
        ));
        http.oauth2ResourceServer(resourceServer -> resourceServer.jwt(withDefaults()));
        return http.build();
    }

    @Bean
    @Order(2)
    public SecurityFilterChain defaultSecurityFilterChain(HttpSecurity http)
            throws Exception {
        http
            .authorizeHttpRequests(authorizeRequests -> authorizeRequests
                    .requestMatchers("/custom-login").permitAll()
                    .requestMatchers("/oauth2/**").permitAll()
                    .requestMatchers("/login/**").permitAll()
                    .anyRequest().authenticated()
            );
             http.formLogin(AbstractHttpConfigurer::disable)
            .csrf(AbstractHttpConfigurer::disable)
            .httpBasic(AbstractHttpConfigurer::disable)
            .sessionManagement(session -> session
                    .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                     .oauth2Login(oauth2 ->
                     oauth2.clientRegistrationRepository(clientRegistrationRepository)
                             .successHandler(socialLoginSuccessHandler)
                             //.authorizedClientService(authorizedClientService())
                     )
            .authenticationProvider(authenticationProvider);
        return http.build();
    }

    @Bean
    public AuthenticationProvider authenticationProvider() {
        return new ExternalUserAuthenticationProvider();
    }
    @Bean
    public RegisteredClientRepository registeredClientRepository(PasswordEncoder passwordEncoder) {
        RegisteredClient oidcClient = RegisteredClient.withId(UUID.randomUUID().toString())
                .clientId(clientId)
                .clientSecret(passwordEncoder.encode(clientSecret))
                .clientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .authorizationGrantType(AuthorizationGrantType.REFRESH_TOKEN)
                .redirectUri(redirectUri)
                .postLogoutRedirectUri("http://127.0.0.1:8082")
                .scope("general_scope")
                .clientSettings(ClientSettings.builder()
                        .requireProofKey(true)
                        .build())
                .build();


        return new InMemoryRegisteredClientRepository(oidcClient);
    }




    @Bean
    public AuthorizationServerSettings authorizationServerSettings() {
        return AuthorizationServerSettings.builder().build();
    }

    @Bean
    public OAuth2AuthorizationService authorizationService() {
        return new InMemoryOAuth2AuthorizationService();
    }

}