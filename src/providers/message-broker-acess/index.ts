/** @format */

import { IRouterMessageBroker } from "./implementations/imessager-broker-acess.interface";
import { RabbitMQ } from "./implementations/rabbit-mq/rabbit-mq.provider";
import { MyQueueRouter } from "./routers/my-queue-router";

const listQueuesListen: Array<IRouterMessageBroker> = [new MyQueueRouter()];

const app = {
  listen: (callback: CallableFunction) => {
    const messagerBrokerAccess = new RabbitMQ();
    listQueuesListen.forEach((queueListener) => {
      queueListener.handle(messagerBrokerAccess);
    });
    callback();
  },
};

export { app };
