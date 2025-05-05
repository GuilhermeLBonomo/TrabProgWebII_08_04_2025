import dotenv from "dotenv";
import { app } from "./providers/web-server";
dotenv.config();
app.listen(process.env.SERVER_PORT, () =>
  console.log(`Servidor rodando na porta ${process.env.SERVER_PORT} ...`)
);
