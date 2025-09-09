package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.repository.GameStateRepository;
import org.springframework.stereotype.Service;

@Service
public class PrePlayService {
    GameStateRepository gameStateRepository;
    public void setLeadPlayer() {

    }

    public void connectUser(long userId) { //todo: 인증로직 추가 필요
        if(gameStateRepository.existsById(userId)) {
            //예외
        }
        else {

        }
    }
}
