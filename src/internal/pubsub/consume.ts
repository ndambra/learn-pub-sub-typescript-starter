import { decode } from "@msgpack/msgpack";
import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const channel = await conn.createChannel();
  const queue = await channel.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: queueType === SimpleQueueType.Transient,
    exclusive: queueType === SimpleQueueType.Transient,
    arguments: {
      "x-dead-letter-exchange": "peril_dlx",
    },
  });
  await channel.bindQueue(queueName, exchange, key);
  return [channel, queue];
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  const deserializer = (data: Buffer): T => {
    const contentsString = data.toString();
    return JSON.parse(contentsString);
  };

  subscribe(conn, exchange, queueName, key, queueType, handler, deserializer);
}

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  const deserializer = (data: Buffer): T => {
    return decode(data) as T;
  };

  subscribe(conn, exchange, queueName, key, queueType, handler, deserializer);
}

export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
): Promise<void> {
  const [channel, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    routingKey,
    simpleQueueType,
  );

  await channel.consume(
    queue.queue,
    async (msg: amqp.ConsumeMessage | null) => {
      if (!msg) return;

      const contents = deserializer(msg.content);

      try {
        const ackType = await handler(contents);
        switch (ackType) {
          case AckType.Ack:
            channel.ack(msg);
            break;
          case AckType.NackRequeue:
            channel.nack(msg, false, true);
            break;
          case AckType.NackDiscard:
            channel.nack(msg, false, false);
            break;
          default:
            const unreachable: never = ackType;
            console.error("Unexpected ack type:", unreachable);
            return;
        }
      } catch (err) {
        console.error("Error handling message:", err);
        channel.nack(msg, false, false);
        return;
      }
    },
  );
}
