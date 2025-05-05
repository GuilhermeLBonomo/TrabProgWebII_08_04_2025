import { Request, Response } from "express";
import { SendCreateUserApplication } from "./send-create-user.application";

export class SendCreateUserController {
  constructor(private readonly sendCreateUser: SendCreateUserApplication) {}

  /**
   * Handle
   * @param req
   * @param resp
   */
  async handle(req: Request, resp: Response): Promise<Response> {
    const { name, email, password, cellPhone } = req.body;
    const currentDate = new Date();
    const userData = {
      name,
      email,
      password,
      cellPhone,
      createdAt: currentDate,
      updatedAt: currentDate,
    };

    try {
      const { code, response } = await this.sendCreateUser.handle(userData);
      return resp.status(code).send(response);
    } catch (error) {
      console.error("Erro ao criar o usuário:", error);
      return resp.status(500).send({
        message: "Erro interno do servidor. Tente novamente mais tarde.",
      });
    }
  }
}
