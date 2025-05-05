import dotenv from "dotenv";
import amqp, { Channel, Connection } from "amqplib";
import { v4 as uuidv4 } from "uuid";
import {
  IMessagerAccess,
  IMessagerAccessRequest,
  IMessagerBrokerAccess,
  IResponseAccessResponse,
} from "../imessager-broker-acess.interface";

dotenv.config();

export class RabbitMQ implements IMessagerBrokerAccess {
  sendPubSub(message: IMessagerAccess): Promise<any> {
    throw new Error("Method not implemented.");
  }
  private readonly URL: string =
    process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

  async connect(): Promise<Channel> {
    try {
      const conn: Connection = await amqp.connect(this.URL);
      return conn.createChannel();
    } catch (err) {
      console.error("Error connecting to RabbitMQ:", err);
      throw err;
    }
  }

  async createQueue(channel: Channel, queue: string): Promise<Channel> {
    try {
      await channel.assertQueue(queue, { durable: true });
      return channel;
    } catch (err) {
      console.error("Error creating queue:", err);
      throw err;
    }
  }

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

  async sendRPC(message: IMessagerAccess): Promise<IResponseAccessResponse> {
    const timeout = Number(process.env.RABBITMQ_TIMEOUT) || 5000;
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

  messageConvert(message: { content: Buffer }): IResponseAccessResponse {
    try {
      const parsed = JSON.parse(message.content.toString());
      return {
        code: typeof parsed.code === "number" ? parsed.code : 200,
        response: parsed,
      };
    } catch (error) {
      return this.createErrorResponse("Invalid JSON format", error);
    }
  }

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

  private createErrorResponse(
    message: string,
    error: any
  ): IResponseAccessResponse {
    return {
      code: 500,
      response: {
        message,
        error: error instanceof Error ? error.stack : String(error),
      },
    };
  }
}
