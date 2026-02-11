const cron = require("node-cron");
const { createApp } = require("./app");
const { config, validateConfig } = require("./config");
const { getPool, closePool } = require("./db");
const { V8BffClient } = require("./clients/v8BffClient");
const { ConsignmentJobService } = require("./services/consignmentJobService");
const { StatusTracker } = require("./services/statusTracker");
const { info, error } = require("./logger");

let server;
let cronTask;

async function bootstrap() {
  validateConfig();
  await getPool(config.db);

  const statusTracker = new StatusTracker({
    host: config.server.host,
    port: config.server.port,
  });

  const v8Client = new V8BffClient(config.v8);
  const jobService = new ConsignmentJobService({
    dbConfig: config.db,
    jobConfig: config.job,
    statusTracker,
    v8Client,
  });

  const app = createApp(jobService, statusTracker);

  server = app.listen(config.server.port, config.server.host, () => {
    statusTracker.setServerStatus(config.server.host, config.server.port);
    info(`API iniciada em ${config.server.host}:${config.server.port}`);
  });

  if (config.job.schedulerEnabled) {
    cronTask = cron.schedule(config.job.schedulerCron, async () => {
      info("Disparando ciclo agendado");
      const result = await jobService.run("scheduler");

      if (!result.ok) {
        error(
          `Ciclo agendado finalizou com erro: ${result.message || "erro_desconhecido"}`
        );
        return;
      }

      info("Ciclo agendado finalizado");
    });

    info(`Scheduler habilitado (${config.job.schedulerCron})`);
  }

  if (config.job.runOnStartup) {
    info("Disparando ciclo inicial no startup");
    const startupResult = await jobService.run("startup");

    if (!startupResult.ok) {
      error(
        `Ciclo inicial finalizou com erro: ${startupResult.message || "erro_desconhecido"}`
      );
    } else {
      info("Ciclo inicial finalizado");
    }
  }
}

async function shutdown(signal) {
  info(`Encerrando aplicacao (${signal})`);

  if (cronTask) {
    cronTask.stop();
  }

  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }

  await closePool();
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

bootstrap().catch(async (err) => {
  error(`Falha ao iniciar API: ${err.message}`);
  await closePool();
  process.exit(1);
});
