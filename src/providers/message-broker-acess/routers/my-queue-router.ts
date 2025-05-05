/** @format */

import { IRouterMessageBroker } from "../implementations/imessager-broker-acess.interface";
import { RabbitMQ } from "../implementations/rabbit-mq/rabbit-mq.provider"; // ou o caminho certo

export class MyQueueRouter implements IRouterMessageBroker {
  /**
   * Handler to handle messages in the message broker queue.
   * @param messagerBrokerAccess - The RabbitMQ or any other broker access instance.
   */
  handle(messagerBrokerAccess: RabbitMQ): void {
    const queue = "myQueue";

    messagerBrokerAccess.listenRPC(queue, (request: any) => {
      console.log("Received message:", request);
      return {
        code: 200,
        response: {
          message: "Processed successfully",
        },
      };
    });
  }
}
