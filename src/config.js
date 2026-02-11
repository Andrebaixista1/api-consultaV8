const dotenv = require("dotenv");

dotenv.config();

function toInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
}

function toBool(value, fallback) {
  if (value === undefined) {
    return fallback;
  }

  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }

  return fallback;
}

const config = {
  server: {
    host: process.env.HOST || "0.0.0.0",
    port: toInt(process.env.PORT, 3000),
  },
  db: {
    host: process.env.DB_HOST || "177.153.62.236",
    port: toInt(process.env.DB_PORT, 1433),
    user: process.env.DB_USER || "andrefelipe",
    password: process.env.DB_PASSWORD || "",
    database: process.env.DB_DATABASE || "apis_v8",
    encrypt: toBool(process.env.DB_ENCRYPT, false),
    trustServerCertificate: toBool(process.env.DB_TRUST_SERVER_CERT, true),
    poolMax: toInt(process.env.DB_POOL_MAX, 10),
    poolMin: toInt(process.env.DB_POOL_MIN, 0),
    poolIdleTimeoutMs: toInt(process.env.DB_POOL_IDLE_TIMEOUT_MS, 30000),
  },
  v8: {
    baseUrl: process.env.V8_BASE_URL || "https://bff.v8sistema.com",
    provider: process.env.V8_PROVIDER || "QI",
    httpTimeoutMs: toInt(process.env.HTTP_TIMEOUT_MS, 30000),
    signerPhone: {
      phoneNumber: process.env.SIGNER_PHONE_NUMBER || "980733602",
      countryCode: process.env.SIGNER_PHONE_COUNTRY_CODE || "55",
      areaCode: process.env.SIGNER_PHONE_AREA_CODE || "11",
    },
  },
  job: {
    waitBetweenApisMs: toInt(
      process.env.WAIT_BETWEEN_APIS_MS ?? process.env.WAIT_BEFORE_AUTHORIZE_MS,
      3000
    ),
    waitBetweenClientsMs: toInt(process.env.WAIT_BETWEEN_CLIENTS_MS, 0),
    maxClientsPerToken: toInt(process.env.MAX_CLIENTS_PER_TOKEN, 250),
    schedulerEnabled: toBool(process.env.SCHEDULER_ENABLED, true),
    schedulerCron: process.env.SCHEDULER_CRON || "0 * * * *",
    runOnStartup: toBool(process.env.JOB_RUN_ON_STARTUP, true),
  },
};

function validateConfig() {
  const missing = [];

  if (!config.db.password) {
    missing.push("DB_PASSWORD");
  }

  if (missing.length > 0) {
    throw new Error(
      `Variaveis obrigatorias nao configuradas: ${missing.join(", ")}`
    );
  }
}

module.exports = {
  config,
  validateConfig,
};
