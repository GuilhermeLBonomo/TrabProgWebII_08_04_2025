/** @format */

import amqp from "amqplib";
import { v4 as uuidv4 } from "uuid";
import {
  IMessagerAccess,
  IMessagerAccessRequest,
  IMessagerBrokerAccess,
  IResponseAccessResponse,
} from "../imessager-broker-acess.interface";

export class RabbitMQ implements IMessagerBrokerAccess {
  private readonly URL: string = "amqp://guest:guest@localhost:5672";

  /**
   * Connect with messager broker
   */
  async connect(): Promise<any> {
    return amqp.connect(this.URL).then((conn) => conn.createChannel());
  }

  /**
   * Listen RPC
   * @param queue
   * @param callback
   */
  listenRPC(queue: string, callback: CallableFunction): void {
    this.connect()
      .then((channel) => this.createQueue(channel, queue))
      .then((ch) => {
        ch.consume(queue, async (msg) => {
          if (!msg) return;

          try {
            const request = this.messageConvertRequest(msg);
            const response = await callback(request);
            await this.responseCallRPC({
              queue,
              replyTo: msg.properties.replyTo,
              correlationId: msg.properties.correlationId,
              response,
            });
            ch.ack(msg);
          } catch (err) {
            console.error("Error handling RPC message:", err);
            ch.nack(msg, false, false);
          }
        });
      })
      .catch((err) => console.error("Error in listenRPC:", err));
  }

  /**
   * Create
   * @param channel
   * @param queue
   */
  async createQueue(
    channel: amqp.Channel,
    queue: string
  ): Promise<amqp.Channel> {
    await channel.assertQueue(queue, { durable: true });
    return channel;
  }

  /**
   * Send Pub/Sub
   * @param queue
   */
  async sendPubSub(message: IMessagerAccess): Promise<void> {
    try {
      const channel = await this.connect().then((ch) =>
        this.createQueue(ch, message.queue)
      );
      channel.sendToQueue(
        message.queue,
        Buffer.from(JSON.stringify(message.message))
      );
    } catch (err) {
      console.error("Error in sendPubSub:", err);
    }
  }

  /**
   * Send RPC
   * @param message
   */
  async sendRPC(message: IMessagerAccess): Promise<IResponseAccessResponse> {
    const timeout = 5000;
    const corr = uuidv4();
    const conn = await amqp.connect(this.URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(message.queue, { durable: true });
    const q = await ch.assertQueue("", { exclusive: true });

    return new Promise((resolve) => {
      let isResponded = false;

      const timer = setTimeout(() => {
        if (!isResponded) {
          conn.close();
          resolve({
            code: 408,
            response: { message: "Timeout" },
          });
        }
      }, timeout);

      ch.consume(
        q.queue,
        (msg) => {
          if (msg?.properties.correlationId === corr) {
            clearTimeout(timer);
            conn.close();
            isResponded = true;
            resolve(this.messageConvert(msg));
          }
        },
        { noAck: true }
      );

      ch.sendToQueue(
        message.queue,
        Buffer.from(JSON.stringify(message.message)),
        {
          correlationId: corr,
          replyTo: q.queue,
        }
      );
    });
  }

  /**
   * Convert Message
   * @param message
   * @returns
   */
  messageConvert(message: { content: Buffer }): IResponseAccessResponse {
    try {
      const parsed = JSON.parse(message.content.toString());

      return {
        code: typeof parsed.code === "number" ? parsed.code : 200,
        response: parsed,
      };
    } catch (error) {
      return {
        code: 500,
        response: {
          message: "Invalid JSON format",
          raw: message.content.toString(),
          error: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }

  /**
   * Message Convert Request
   * @param message
   * @returns
   */
  messageConvertRequest(message: { content: Buffer }): IMessagerAccessRequest {
    try {
      const parsed = JSON.parse(message.content.toString());

      return {
        body: parsed,
        message: "Parsed successfully",
      };
    } catch (error) {
      const errorMsg =
        error instanceof Error ? error.message : "Unknown parsing error";

      return {
        body: null,
        message: `Invalid JSON (${errorMsg}): ${message.content.toString()}`,
      };
    }
  }

  /**
   * Response RPC
   * @param replyTo
   * @param correlationId
   * @param response
   * @returns
   */
  async responseCallRPC(objResponse: {
    queue: string;
    replyTo: string;
    correlationId: string;
    response: IResponseAccessResponse;
  }): Promise<void> {
    try {
      const channel = await this.connect().then((ch) =>
        this.createQueue(ch, objResponse.queue)
      );
      channel.sendToQueue(
        objResponse.replyTo,
        Buffer.from(JSON.stringify(objResponse.response)),
        { correlationId: objResponse.correlationId }
      );
    } catch (err) {
      console.error("Error in responseCallRPC:", err);
    }
  }
}
