import amqp from "amqplib";
import { publishGameLog } from "../../client/index.js";
import { getMaliciousLog } from "./gamelogic.js";
import type { GameState } from "./gamestate.js";

export function commandSpam(ch: amqp.ConfirmChannel, gs: GameState, words: string[]) {
  if (words.length < 2) {
    throw new Error("Usage: spam <number>");
  }

  const spamNumStr = words[1];
  if (!spamNumStr) {
    throw new Error("Usage: spam <number>");
  }

  const spamNum = +spamNumStr;
  for (let i = 0; i < spamNum; i++) {
    const malLog = getMaliciousLog();
    publishGameLog(ch, gs.getUsername(), malLog);
  }
}
