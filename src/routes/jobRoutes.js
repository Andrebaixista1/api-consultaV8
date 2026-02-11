const express = require("express");

function createJobRoutes(jobService) {
  const router = express.Router();

  router.post("/run", async (req, res) => {
    const result = await jobService.run("manual");
    if (!result.ok && result.reason === "already_running") {
      return res.status(409).json(result);
    }

    if (!result.ok) {
      return res.status(500).json(result);
    }

    return res.status(200).json(result);
  });

  return router;
}

module.exports = {
  createJobRoutes,
};
