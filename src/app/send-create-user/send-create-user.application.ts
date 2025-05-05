import { IMessagerBrokerAccess } from "../../providers/message-broker-acess/implementations/imessager-broker-acess.interface";
import { ISendCreateUserDTO } from "./isend-create-user-dto.interface";
import dotenv from "dotenv";

dotenv.config();

export class SendCreateUserApplication {
  private readonly queue: string;

  constructor(private readonly messagerBroker: IMessagerBrokerAccess) {
    this.queue = process.env.RABBIT_MYQUEUE ?? "";
    if (!this.queue) {
      throw new Error("A variável de ambiente RABBIT_MYQUEUE está ausente.");
    }
  }

  /**
   * Envia solicitação para criar um novo usuário via RPC.
   *
   * @param userSend Dados do usuário a ser criado.
   * @returns Código HTTP e resposta do broker.
   */
  async handle(
    userSend: ISendCreateUserDTO
  ): Promise<{ code: number; response: any }> {
    try {
      const response = await this.messagerBroker.sendRPC({
        queue: this.queue,
        message: userSend,
      });

      return response;
    } catch (error) {
      console.error("Erro ao enviar mensagem para criação de usuário:", error);

      return {
        code: 500,
        response: {
          message: "Erro ao tentar criar usuário via broker.",
          error: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }
}
