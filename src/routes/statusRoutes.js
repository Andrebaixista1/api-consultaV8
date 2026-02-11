const express = require("express");

function createStatusRoutes(statusTracker) {
  const router = express.Router();

  router.get("/", (_req, res) => {
    return res.status(200).json(statusTracker.getStatus());
  });

  return router;
}

module.exports = {
  createStatusRoutes,
};
