import express, { Request, Response } from "express";
import { sendCreateUserController } from "../../../../app/send-create-user";
import { IRouterHttp } from "../routes";

export class UserRouter implements IRouterHttp {
  /**
   * Handler
   * @param router
   */
  handle(router: express.Router): void {
    router.post("/users", async (req: Request, resp: Response) => {
      try {
        return await sendCreateUserController.handle(req, resp);
      } catch (error) {
        console.error("Error creating user:", error);
        resp.status(500).json({
          message: "Internal Server Error",
          error: error instanceof Error ? error.message : String(error),
        });
      }
    });
  }
}
