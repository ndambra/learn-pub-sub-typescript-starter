import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Peril game server connected to RabbitMQ.");

  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("Peril server connection to RabbitMQ closed");
      } catch (error) {
        console.error("Error closing RabbitMQ connection: ", error);
      } finally {
        process.exit(0);
      }
    }),
  );

  await declareAndBind(
    conn, 
    ExchangePerilTopic, 
    GameLogSlug, 
    `${GameLogSlug}.*`, 
    SimpleQueueType.Durable
  );

  const confirmChannel = await conn.createConfirmChannel();

  printServerHelp();
  while (true) {
    const words = await getInput();
    if (words.length === 0) continue;

    const command = words[0];
    switch (command) {
      case "pause":
        console.log("Sending pause message...");
        try {
          await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, { isPaused: true });
        } catch (error) {
          console.error("error publishing pause message: ", error);
        }
        break;
      case "resume":
        console.log("Sending resume message...");
        try {
          await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, { isPaused: false });
        } catch (error) {
          console.error("error publishing resume message: ", error);
        }
        break;
      case "quit":
        console.log("Exiting...");
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
