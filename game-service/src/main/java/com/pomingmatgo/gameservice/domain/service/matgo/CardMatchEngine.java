package com.pomingmatgo.gameservice.domain.service.matgo;

import com.pomingmatgo.gameservice.domain.ChoiceInfo;
import com.pomingmatgo.gameservice.domain.Player;
import com.pomingmatgo.gameservice.domain.card.Card;
import com.pomingmatgo.gameservice.domain.card.CardType;
import com.pomingmatgo.gameservice.domain.card.SpecialType;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * 고스톱 카드 매칭 규칙의 순수 계산부. 저장소 접근 없이 입력(바닥 스택, 상대 획득패)만으로
 * 턴 결과와 바닥에 적용할 변경(FloorEffect)을 결정한다. 상태 조회/반영은 GameService 담당.
 */
@Component
public class CardMatchEngine {

    public sealed interface FloorEffect {
        record Place(List<Card> cards) implements FloorEffect {}
        record ClearMonth(int month) implements FloorEffect {}
        record Remove(Card card) implements FloorEffect {}
    }

    /** pendingChoice는 result.isChoiceRequired()일 때만 non-null */
    public record MatchOutcome(ProcessCardResult result, List<FloorEffect> effects, ChoiceInfo pendingChoice) {}

    private record PlayOutcome(ProcessCardResult result, List<FloorEffect> effects) {}

    public MatchOutcome decideSubmit(Player currentPlayer, Card submitted, Card turned,
                                     List<Card> submittedStack, List<Card> turnedStack, List<Card> opponentAcquired) {
        if (turned.hasSameMonthAs(submitted)) {
            return decideSameMonth(submitted, turned, submittedStack, opponentAcquired);
        }

        PlayOutcome first = decidePlay(submitted, submittedStack, opponentAcquired, List.of());
        if (first.result().isChoiceRequired()) {
            return pendingOutcome(currentPlayer, submitted, turned, submittedStack, null, first.effects());
        }

        // 같은 턴에서 두 번 피를 뺏으면(둘 다 3장 스택) 같은 피가 중복 선택되지 않도록 앞선 뺏기를 제외한다
        PlayOutcome second = decidePlay(turned, turnedStack, opponentAcquired, first.result().getMoveCards());
        List<FloorEffect> effects = concat(first.effects(), second.effects());
        if (second.result().isChoiceRequired()) {
            return pendingOutcome(currentPlayer, turned, null, turnedStack, first.result(), effects);
        }
        return new MatchOutcome(first.result().merge(second.result()), effects, null);
    }

    public MatchOutcome decideFloorSelection(Player currentPlayer, ChoiceInfo choiceInfo, Card chosen,
                                             List<Card> turnedStack, List<Card> opponentAcquired) {
        Card submitted = choiceInfo.getSubmittedCard();
        Card turned = choiceInfo.getTurnedCard();
        ProcessCardResult base = ProcessCardResult.immediate(List.of(chosen, submitted));
        ProcessCardResult prev = restorePrev(choiceInfo);
        // 낸 카드는 선택 대기 진입 시 바닥에 저장되지 않고 choiceInfo로만 이월되므로 바닥 삭제 대상은 선택된 카드뿐
        FloorEffect removeChosen = new FloorEffect.Remove(chosen);

        if (turned == null) {
            return new MatchOutcome(prev.merge(base), List.of(removeChosen), null);
        }

        ProcessCardResult carryover = prev.merge(base);
        PlayOutcome next = decidePlay(turned, turnedStack, opponentAcquired, carryover.getMoveCards());
        List<FloorEffect> effects = concat(next.effects(), List.of(removeChosen));
        if (next.result().isChoiceRequired()) {
            return pendingOutcome(currentPlayer, turned, null, turnedStack, carryover, effects);
        }
        return new MatchOutcome(prev.merge(base.merge(next.result())), effects, null);
    }

    private MatchOutcome decideSameMonth(Card submitted, Card turned, List<Card> stack, List<Card> opponentAcquired) {
        if (stack.size() == 1) {
            return new MatchOutcome(ProcessCardResult.ppeok(List.of()),
                    List.of(new FloorEffect.Place(List.of(turned, submitted))), null);
        }

        List<Card> acquired = new ArrayList<>(List.of(turned, submitted));
        acquired.addAll(stack);
        List<FloorEffect> effects = List.of(new FloorEffect.ClearMonth(turned.getMonth()));

        Optional<Card> pi = findMovablePi(opponentAcquired, List.of());
        if (pi.isEmpty()) {
            return new MatchOutcome(ProcessCardResult.immediate(acquired), effects, null);
        }
        acquired.add(pi.get());
        // 스택이 비어 있었다면 쪽(낸 카드+뒤집은 카드만 매치), 2장이었다면 따닥. 1장(뻑)은 위에서 걸러졌다
        ProcessCardResult result = stack.isEmpty()
                ? ProcessCardResult.jjok(acquired, pi.get())
                : ProcessCardResult.ttadak(acquired, pi.get());
        return new MatchOutcome(result, effects, null);
    }

    private PlayOutcome decidePlay(Card card, List<Card> stack, List<Card> opponentAcquired, List<Card> alreadyTakenPi) {
        int month = card.getMonth();
        return switch (stack.size()) {
            case 0 -> new PlayOutcome(ProcessCardResult.immediate(List.of()),
                    List.of(new FloorEffect.Place(List.of(card))));
            case 1 -> new PlayOutcome(ProcessCardResult.immediate(withCard(stack, card)),
                    List.of(new FloorEffect.ClearMonth(month)));
            case 2 -> new PlayOutcome(ProcessCardResult.choicePending(stack), List.of());
            case 3 -> {
                List<Card> acquired = withCard(stack, card);
                List<FloorEffect> effects = List.of(new FloorEffect.ClearMonth(month));
                yield findMovablePi(opponentAcquired, alreadyTakenPi)
                        .map(pi -> {
                            acquired.add(pi);
                            return new PlayOutcome(ProcessCardResult.claimOpponentPi(acquired, pi), effects);
                        })
                        .orElseGet(() -> new PlayOutcome(ProcessCardResult.immediate(acquired), effects));
            }
            default -> new PlayOutcome(ProcessCardResult.immediate(List.of()), List.of());
        };
    }

    private MatchOutcome pendingOutcome(Player currentPlayer, Card submittedCard, Card turnedCard,
                                        List<Card> selectableCards, ProcessCardResult prev, List<FloorEffect> effects) {
        ChoiceInfo choiceInfo = ChoiceInfo.builder()
                .playerNumToChoose(currentPlayer)
                .submittedCard(submittedCard)
                .selectableCards(selectableCards)
                .turnedCard(turnedCard)
                .prevCards(prev != null ? prev.getAcquiredCards() : null)
                .prevMoveCards(prev != null ? prev.getMoveCards() : null)
                .build();
        return new MatchOutcome(ProcessCardResult.choicePending(selectableCards), effects, choiceInfo);
    }

    private ProcessCardResult restorePrev(ChoiceInfo choiceInfo) {
        return ProcessCardResult.builder()
                .acquiredCards(new ArrayList<>(nullSafe(choiceInfo.getPrevCards())))
                .moveCards(new ArrayList<>(nullSafe(choiceInfo.getPrevMoveCards())))
                .build();
    }

    private Optional<Card> findMovablePi(List<Card> opponentAcquired, List<Card> excluded) {
        return opponentAcquired.stream()
                .filter(c -> c.getType() == CardType.PI)
                .filter(c -> !excluded.contains(c))
                .min(Comparator.comparing(c -> c.getSpecialType() == SpecialType.SSANG_PI));
    }

    private static List<Card> withCard(List<Card> stack, Card card) {
        List<Card> cards = new ArrayList<>(stack);
        cards.add(card);
        return cards;
    }

    private static List<Card> nullSafe(List<Card> cards) {
        return cards != null ? cards : List.of();
    }

    private static List<FloorEffect> concat(List<FloorEffect> a, List<FloorEffect> b) {
        List<FloorEffect> merged = new ArrayList<>(a);
        merged.addAll(b);
        return merged;
    }
}
