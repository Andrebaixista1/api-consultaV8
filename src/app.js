const express = require("express");
const { createJobRoutes } = require("./routes/jobRoutes");
const { createStatusRoutes } = require("./routes/statusRoutes");

function createApp(jobService, statusTracker) {
  const app = express();
  app.use(express.json({ limit: "1mb" }));

  app.get("/health", (_req, res) => {
    return res.status(200).json({ ok: true, service: "v8-consignment-api" });
  });

  app.use("/api/jobs", createJobRoutes(jobService));
  app.use("/api/status", createStatusRoutes(statusTracker));

  return app;
}

module.exports = {
  createApp,
};
