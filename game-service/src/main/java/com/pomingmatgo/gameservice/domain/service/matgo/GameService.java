package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.repository.InstalledCardRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class GameService {
    private final InstalledCardRepository installedCardRepository;
    /*public Mono<Integer> getConfusedPlayer(long roomId) {

    }*/
}
