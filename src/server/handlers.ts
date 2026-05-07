import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";
import { AckType } from "../internal/pubsub/consume.js";

export function handlerLogs(): (log: GameLog) => Promise<AckType> {
  return async (log: GameLog): Promise<AckType> => {
    await writeLog(log);
    process.stdout.write("> ");
    // TODO: Figure out
  }
}