import { RabbitMQ } from "../../providers/message-broker-acess/implementations/rabbit-mq/rabbit-mq.provider";
import { SendCreateUserApplication } from "./send-create-user.application";
import { SendCreateUserController } from "./send-create-user.controller";

const messagerBroker = new RabbitMQ();
const sendCreateUserApp = new SendCreateUserApplication(messagerBroker);
const sendCreateUserController = new SendCreateUserController(
  sendCreateUserApp
);

export { sendCreateUserApp, sendCreateUserController };

// curl -X POST http://localhost:3000/api/users -H "Content-Type: application/json" -d '{"name":"Alice","email":"alice@example.com","password":"secure123","cellPhone":"1234567890"}'
