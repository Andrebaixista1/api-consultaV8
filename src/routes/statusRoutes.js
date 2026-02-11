const express = require("express");

function createStatusRoutes(statusTracker, jobService) {
  const router = express.Router();

  router.get("/", (_req, res) => {
    const baseStatus = statusTracker.getStatus();
    const tokensSummary =
      jobService && typeof jobService.getStatusSnapshot === "function"
        ? jobService.getStatusSnapshot()
        : null;

    return res.status(200).json({
      ...baseStatus,
      tokens_summary: tokensSummary,
    });
  });

  return router;
}

module.exports = {
  createStatusRoutes,
};
