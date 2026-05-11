import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";
import { AckType } from "../internal/pubsub/consume.js";

export function handlerLogs(): (log: GameLog) => Promise<AckType> {
  return async (log: GameLog): Promise<AckType> => {
    try {
      await writeLog(log);
      return AckType.Ack;
    } catch (err) {
      console.error("Error writing log:", err);
      return AckType.NackDiscard;
    } finally  {
      process.stdout.write("> ");
    }
  }
}