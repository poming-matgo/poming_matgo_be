package com.pomingmatgo.userservice.domain.user.mapper;


import com.pomingmatgo.userservice.api.user.request.RegisterRequest;
import com.pomingmatgo.userservice.domain.user.UserTmp;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

@Mapper(componentModel = "spring")
public interface UserTmpMapper {
    UserTmpMapper INSTANCE = Mappers.getMapper(UserTmpMapper.class);


    @Mapping(target = "authId", source = "randomString")
    @Mapping(target = "password", source = "encodedPassword")
    @Mapping(target = "identifier", source = "request.email")
    UserTmp toUserTmp(RegisterRequest request, String randomString, String encodedPassword);
}