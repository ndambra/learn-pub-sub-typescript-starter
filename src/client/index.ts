import amqp from "amqplib";
import { SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import {
  ArmyMovesPrefix,
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
  PauseKey,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import {
  clientWelcome,
  commandStatus,
  getInput,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

export async function publishGameLog(
  ch: amqp.ConfirmChannel,
  username: string,
  message: string,
) {
  const gameLog: GameLog = {
    currentTime: new Date(),
    message,
    username
  };
  return publishMsgPack(
    ch,
    ExchangePerilTopic,
    `${GameLogSlug}.${username}`,
    gameLog
  )
}

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Peril game client connected to RabbitMQ.");

  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("Peril client connection to RabbitMQ closed");
      } catch (error) {
        console.error("Error closing RabbitMQ connection: ", error);
      } finally {
        process.exit(0);
      }
    }),
  );

  const publishChannel = await conn.createConfirmChannel();
  const username = await clientWelcome();
  const pauseUserQueue = `${PauseKey}.${username}`;

  const gameState = new GameState(username);
  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    pauseUserQueue,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gameState),
  );
  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gameState, publishChannel),
  );
  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    WarRecognitionsPrefix,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    handlerWar(gameState, publishChannel),
  );

  while (true) {
    const words = await getInput();
    if (words.length === 0) continue;

    const command = words[0];
    switch (command) {
      case "spawn":
        try {
          commandSpawn(gameState, words);
        } catch (error) {
          console.log((error as Error).message);
        }
        break;
      case "move":
        try {
          const armyMove = commandMove(gameState, words);
          publishJSON(
            publishChannel,
            ExchangePerilTopic,
            `${ArmyMovesPrefix}.${username}`,
            armyMove,
          );
        } catch (error) {
          console.log((error as Error).message);
        }
        break;
      case "status":
        await commandStatus(gameState);
        break;
      case "help":
        printClientHelp();
        break;
      case "spam":
        console.log("Spamming not allowed yet!");
        break;
      case "quit":
        printQuit();
        process.exit(0);
      default:
        console.log("Unknown command");
        break;
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
