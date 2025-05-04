import { RabbitMQ } from "../../providers/message-broker-acess/implementations/rabbit-mq/rabbit-mq.provider";
import { SendCreateUserApplication } from "./send-create-user.application";
import { SendCreateUserController } from "./send-create-user.controller";

const messagerBroker = new RabbitMQ();
const sendCreateUserApp = new SendCreateUserApplication(messagerBroker);
const sendCreateUserController = new SendCreateUserController(
  sendCreateUserApp
);

export { sendCreateUserApp, sendCreateUserController };

//curl --request POST --url 'http://localhost:3000/user' --header 'Content-Type: application/json' --data '{"name": "User Teste", "email": "email@teste.com", "password": "senha_fraca", "cellPhone": "+55999999999"}'
