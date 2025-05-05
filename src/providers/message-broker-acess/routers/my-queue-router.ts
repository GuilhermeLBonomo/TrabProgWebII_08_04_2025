import dotenv from "dotenv";
import { IRouterMessageBroker } from "../implementations/imessager-broker-acess.interface";
import { RabbitMQ } from "../implementations/rabbit-mq/rabbit-mq.provider";

dotenv.config();

interface IRequest {
  body: any;
  message: string;
}

export class MyQueueRouter implements IRouterMessageBroker {
  private readonly messagerBrokerAccess: RabbitMQ;

  constructor(messagerBrokerAccess: RabbitMQ) {
    this.messagerBrokerAccess = messagerBrokerAccess;
  }

  handle(): void {
    const queue = process.env.QUEUE ?? "myQueue";

    this.messagerBrokerAccess.listenRPC(queue, (request: IRequest) => {
      try {
        console.log("Received message:", request);
        if (!request.body) {
          throw new Error("Request body is missing");
        }
        return {
          code: 200,
          response: {
            message: "Processed successfully",
          },
        };
      } catch (error: any) {
        console.error("Error processing request:", error);

        return {
          code: 400,
          response: {
            message: error.message ?? "Internal error",
          },
        };
      }
    });
  }
}
