import amqp from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState): AckType => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
}

export function handlerMove(
    gs: GameState,
    ch: amqp.ConfirmChannel
): (mv: ArmyMove) => Promise<AckType> {
    return async (mv: ArmyMove): Promise<AckType> => {
        try {
            const moveOutcome = handleMove(gs, mv);
            switch (moveOutcome) {
                case MoveOutcome.Safe:
                case MoveOutcome.SamePlayer:
                    return AckType.Ack;
                case MoveOutcome.MakeWar:
                    const rw: RecognitionOfWar = {
                        attacker: mv.player,
                        defender: gs.getPlayerSnap(),
                    };
                    try {
                        await publishJSON(
                            ch,
                            ExchangePerilTopic,
                            `${WarRecognitionsPrefix}.${gs.getUsername()}`,
                            rw
                        );
                        return AckType.Ack;
                    } catch (error) {
                        console.error("Error publishing war recognition:", error);
                        return AckType.NackRequeue;
                    }
                default:
                    return AckType.NackDiscard;
            }
        } finally {
            process.stdout.write("> ");
        }
    };
}

export function handlerWar(
    gs: GameState
): (rw: RecognitionOfWar) => Promise<AckType> {
    return async (rw: RecognitionOfWar): Promise<AckType> => {
        try {
            const warRes = handleWar(gs, rw);
            switch (warRes.result) {
                case WarOutcome.NoUnits:
                    return AckType.NackDiscard;
                case WarOutcome.NotInvolved:
                    return AckType.NackRequeue;
                case WarOutcome.OpponentWon:
                case WarOutcome.YouWon:
                case WarOutcome.Draw:
                    return AckType.Ack;
                default:
                    const unreachable: never = warRes;
                    console.log("Unrecognized war resolution:", unreachable);
                    return AckType.NackDiscard;
            }
        } finally {
            process.stdout.write("> ");
        }

    };
}