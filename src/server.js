"use strict";

const express = require("express");
const dotenv = require("dotenv");
const axios = require("axios");
const sql = require("mssql");
const fs = require("fs");
const path = require("path");

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

function nowIso() {
  return new Date().toISOString();
}

function log(level, message, meta = {}) {
  const parts = Object.entries(meta)
    .filter(([, value]) => value !== undefined)
    .map(([key, value]) => `${key}=${String(value)}`);
  const suffix = parts.length ? ` | ${parts.join(" | ")}` : "";
  const line = `[${nowIso()}] ${level.toUpperCase()} ${message}${suffix}`;
  if (level === "error") {
    console.error(line);
    return;
  }
  console.log(line);
}

function info(message, meta = {}) {
  log("info", message, meta);
}

function error(message, meta = {}) {
  log("error", message, meta);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeCpf(value) {
  const digits = String(value || "").replace(/\D/g, "");
  if (!digits || digits.length > 11) {
    return null;
  }
  return digits.padStart(11, "0");
}

function toBirthDate(value) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toISOString().slice(0, 10);
}

function toIsoNoMs(date) {
  return `${date.toISOString().split(".")[0]}Z`;
}

function utcDayRange() {
  const now = new Date();
  const start = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0)
  );
  const end = new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1000);

  return {
    startDate: toIsoNoMs(start),
    endDate: toIsoNoMs(end),
  };
}

function parseSignerPhone(rawPhone, fallback) {
  const onlyDigits = String(rawPhone || "").replace(/\D/g, "");
  if (!onlyDigits) {
    return fallback;
  }

  let digits = onlyDigits;
  let countryCode = fallback.countryCode || "55";

  if (digits.startsWith("55") && digits.length >= 12) {
    countryCode = "55";
    digits = digits.slice(2);
  }

  if (digits.length > 11) {
    digits = digits.slice(-11);
  }

  if (digits.length === 11 || digits.length === 10) {
    return {
      countryCode,
      areaCode: digits.slice(0, 2),
      phoneNumber: digits.slice(2),
    };
  }

  if (digits.length === 9 || digits.length === 8) {
    return {
      countryCode,
      areaCode: fallback.areaCode,
      phoneNumber: digits,
    };
  }

  return fallback;
}

function parseMarginValue(raw) {
  if (raw === undefined || raw === null || raw === "") {
    return null;
  }

  if (typeof raw === "number" && Number.isFinite(raw)) {
    return Number(raw.toFixed(2));
  }

  let value = String(raw).replace(/\s+/g, "");
  if (!value) {
    return null;
  }

  const hasDot = value.includes(".");
  const hasComma = value.includes(",");

  if (hasDot && hasComma) {
    if (value.lastIndexOf(",") > value.lastIndexOf(".")) {
      value = value.replace(/\./g, "").replace(",", ".");
    } else {
      value = value.replace(/,/g, "");
    }
  } else if (hasComma) {
    value = value.replace(",", ".");
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }

  return Number(parsed.toFixed(2));
}

function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch (_err) {
    return String(value);
  }
}

function extractApiErrorMessage(payload) {
  if (payload === undefined || payload === null) {
    return "";
  }

  if (typeof payload === "string") {
    return payload.trim();
  }

  const directKeys = [
    "detail",
    "description",
    "message",
    "error_description",
    "title",
    "error",
  ];
  for (const key of directKeys) {
    const value = payload?.[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }

  if (Array.isArray(payload?.errors) && payload.errors.length > 0) {
    const first = payload.errors[0];
    if (typeof first === "string" && first.trim()) {
      return first.trim();
    }
    if (first && typeof first === "object") {
      for (const key of directKeys) {
        const value = first[key];
        if (typeof value === "string" && value.trim()) {
          return value.trim();
        }
      }
    }
  }

  if (payload?.data && typeof payload.data === "object") {
    for (const key of directKeys) {
      const value = payload.data[key];
      if (typeof value === "string" && value.trim()) {
        return value.trim();
      }
    }
  }

  const serialized = safeStringify(payload);
  if (serialized && serialized !== "{}" && serialized !== "[]") {
    return serialized.slice(0, 2000);
  }

  return "";
}

function isConsultAlreadyExistsError(payload) {
  if (!payload || typeof payload !== "object") {
    return false;
  }

  const type = String(payload.type || "").trim().toLowerCase();
  if (type === "consult_already_exists_by_user_and_document_number") {
    return true;
  }

  const text = [
    payload.title,
    payload.detail,
    payload.message,
    payload.error_description,
  ]
    .filter(Boolean)
    .map((value) => String(value).toLowerCase())
    .join(" ");

  return text.includes("ja existe uma consulta ativa");
}

function compactMessage(payload, fallbackMessage) {
  let text = safeStringify(payload);
  if (!text || text === "undefined") {
    text = fallbackMessage ? String(fallbackMessage) : "";
  }
  if (text.length > 30000) {
    return text.slice(0, 30000);
  }
  return text;
}

function statusToDbValue(rawStatus) {
  const status = String(rawStatus || "").trim();
  if (!status) {
    return "Pendente";
  }
  const normalized = normalizeStatusToken(status);
  const translatedMap = {
    WAITING_CONSENT: "Aguardando Consentimento",
    CONSENT_APPROVED: "Consentimento Aprovado",
    WAITING_CONSULT: "Aguardando Consulta",
    WAITING_CREDIT_ANALYSIS: "Aguardando Analise de Credito",
    SUCCESS: "Sucesso",
    FAILED: "Falha",
    REJECTED: "Rejeitado",
  };
  if (translatedMap[normalized]) {
    return translatedMap[normalized];
  }
  return normalizeStatusForDb(status, "Pendente");
}

const NO_RETRY_FINAL_STATUS_SET = new Set([
  "WAITING_CONSENT",
  "SUCCESS",
  "SUCESSO",
  "FAILED",
  "FALHA",
  "REJECTED",
  "REJEITADO",
  "AGUARDANDO CONSENTIMENTO",
  "SUCESSO",
  "REJEITADO",
]);

const RESULT_ONLY_RETRY_STATUS_SET = new Set([
  "CONSENT_APPROVED",
  "WAITING_CONSULT",
  "WAITING_CREDIT_ANALYSIS",
  "CONSENTIMENTO APROVADO",
  "AGUARDANDO CONSULTA",
  "AGUARDANDO ANALISE DE CREDITO",
]);

function normalizeStatusToken(value) {
  return String(value || "").trim().toUpperCase();
}

function isNoRetryFinalStatus(value) {
  return NO_RETRY_FINAL_STATUS_SET.has(normalizeStatusToken(value));
}

function isResultOnlyRetryStatus(value) {
  return RESULT_ONLY_RETRY_STATUS_SET.has(normalizeStatusToken(value));
}

function descriptionToMessage(descriptionValue, fallbackMessage = null) {
  if (descriptionValue === undefined || descriptionValue === null) {
    if (!fallbackMessage) {
      return null;
    }
    return String(fallbackMessage).slice(0, 30000);
  }
  const text = String(descriptionValue).trim();
  if (!text) {
    if (!fallbackMessage) {
      return null;
    }
    return String(fallbackMessage).slice(0, 30000);
  }
  return text.slice(0, 30000);
}

function isIndividual(client) {
  return String(client.tipoConsulta || "").trim().toLowerCase() === "individual";
}

function normalizeKey(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeStatusForDb(status, fallback = "Pendente") {
  const value = String(status || "").trim();
  if (!value) {
    return fallback;
  }
  if (value.length <= 50) {
    return value;
  }
  return value.slice(0, 50);
}

const config = {
  server: {
    host: process.env.HOST || "0.0.0.0",
    port: toInt(process.env.PORT, 3002),
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
  auth: {
    tokenUrl:
      process.env.V8_AUTH_TOKEN_URL || "https://auth.v8sistema.com/oauth/token",
    audience: process.env.V8_AUTH_AUDIENCE || "https://bff.v8sistema.com",
    scope: process.env.V8_AUTH_SCOPE || "online_access",
    clientId:
      process.env.V8_AUTH_CLIENT_ID || "DHWogdaYmEI8n5bwwxPDzulMlSK7dwIn",
    grantType: process.env.V8_AUTH_GRANT_TYPE || "password",
    cookie: process.env.V8_AUTH_COOKIE || "",
    timeoutMs: toInt(process.env.V8_AUTH_TIMEOUT_MS, 30000),
  },
  consultDay: {
    refreshAfterMs: toInt(process.env.CONSULT_DAY_REFRESH_AFTER_MS, 3600000),
    defaultTotal: toInt(process.env.CONSULT_DAY_DEFAULT_TOTAL, 250),
  },
  worker: {
    pollIntervalMs: 10000,
    fetchLimit: toInt(process.env.V8_QUEUE_FETCH_LIMIT, 300),
    waitBetweenApisMs: toInt(process.env.WAIT_BETWEEN_APIS_MS, 3000),
    waitBetweenClientsMs: toInt(process.env.WAIT_BETWEEN_CLIENTS_MS, 0),
    retryBetweenAttemptsMs: 3000,
    retryCooldownMs: toInt(process.env.V8_RETRY_COOLDOWN_MS, 45000),
    maxAttempts: toInt(process.env.V8_SUCCESS_MAX_ATTEMPTS, 5),
    tokenCacheMs: toInt(process.env.V8_TOKEN_CACHE_MS, 60000),
    resultOnlyRetryLogPath:
      process.env.V8_RESULT_ONLY_RETRY_LOG_PATH ||
      path.join(__dirname, "tmp", "result_only_retry.log"),
  },
};

if (!config.db.password) {
  throw new Error("DB_PASSWORD nao configurada");
}

let poolPromise;

function createSqlConfig(dbConfig) {
  return {
    server: dbConfig.host,
    port: dbConfig.port,
    user: dbConfig.user,
    password: dbConfig.password,
    database: dbConfig.database,
    pool: {
      max: dbConfig.poolMax,
      min: dbConfig.poolMin,
      idleTimeoutMillis: dbConfig.poolIdleTimeoutMs,
    },
    options: {
      encrypt: dbConfig.encrypt,
      trustServerCertificate: dbConfig.trustServerCertificate,
      enableArithAbort: true,
    },
  };
}

async function getPool() {
  if (!poolPromise) {
    const pool = new sql.ConnectionPool(createSqlConfig(config.db));
    poolPromise = pool.connect();
  }
  return poolPromise;
}

async function closePool() {
  if (!poolPromise) {
    return;
  }
  const pool = await poolPromise;
  await pool.close();
  poolPromise = undefined;
}

const v8Http = axios.create({
  baseURL: config.v8.baseUrl,
  timeout: config.v8.httpTimeoutMs,
  validateStatus: () => true,
});

function buildHeaders(accessToken) {
  return {
    Authorization: `Bearer ${accessToken}`,
  };
}

function toSafeInt(value, fallback = 0) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

async function requestV8AccessToken(username, password) {
  const body = new URLSearchParams();
  body.append("grant_type", config.auth.grantType);
  body.append("username", username);
  body.append("password", password);
  body.append("audience", config.auth.audience);
  body.append("scope", config.auth.scope);
  body.append("client_id", config.auth.clientId);

  const headers = {
    "Content-Type": "application/x-www-form-urlencoded",
  };
  if (config.auth.cookie) {
    headers.Cookie = config.auth.cookie;
  }

  const response = await axios.post(config.auth.tokenUrl, body.toString(), {
    timeout: config.auth.timeoutMs,
    headers,
    validateStatus: () => true,
  });

  const status = Number(response.status || 0);
  const accessToken = String(response.data?.access_token || "").trim();
  if (status >= 400 || !accessToken) {
    const apiError =
      response.data?.error_description ||
      response.data?.error ||
      response.data?.message ||
      "Falha ao gerar token no auth";
    throw new Error(
      `Falha auth V8 (HTTP_${status || "ERR"}): ${String(apiError).slice(0, 200)}`
    );
  }

  return {
    accessToken,
    expiresIn: Number(response.data?.expires_in || 0),
    tokenType: String(response.data?.token_type || ""),
  };
}

async function createConsult(accessToken, client) {
  const body = {
    borrowerDocumentNumber: String(client.cliente_cpf || ""),
    gender: String(client.cliente_sexo || ""),
    birthDate: toBirthDate(client.nascimento),
    signerName: String(client.cliente_nome || ""),
    signerEmail: String(client.email || ""),
    signerPhone: parseSignerPhone(client.telefone, config.v8.signerPhone),
    provider: config.v8.provider,
  };

  return v8Http.post("/private-consignment/consult", body, {
    headers: buildHeaders(accessToken),
  });
}

async function authorizeConsult(accessToken, consultId) {
  return v8Http.post(`/private-consignment/consult/${consultId}/authorize`, {}, {
    headers: buildHeaders(accessToken),
  });
}

async function getConsultResult(accessToken, cpf) {
  const { startDate, endDate } = utcDayRange();
  return v8Http.get("/private-consignment/consult", {
    headers: buildHeaders(accessToken),
    params: {
      startDate,
      endDate,
      limit: 50,
      page: 1,
      search: String(cpf || ""),
      provider: config.v8.provider,
    },
  });
}

function parseConsultPayload(payload) {
  const rows = Array.isArray(payload?.data) ? payload.data : [];
  const parsedRows = rows.map((row) => {
    const rawStatus = String(row?.status || "").trim();
    return {
      statusRaw: rawStatus,
      statusDb: statusToDbValue(rawStatus),
      description: row?.description === undefined || row?.description === null
        ? null
        : String(row.description),
      marginValue: parseMarginValue(row?.availableMarginValue),
      isSuccess: rawStatus.toUpperCase() === "SUCCESS",
    };
  });
  const successRow = parsedRows.find((row) => row.isSuccess);
  const firstRow = parsedRows[0] || null;
  const selectedRow = successRow || firstRow || null;

  return {
    hasSuccess: Boolean(successRow),
    marginValue: selectedRow ? selectedRow.marginValue : null,
    statusText: selectedRow ? selectedRow.statusRaw : null,
    descriptionText: selectedRow ? selectedRow.description : null,
    totalRows: parsedRows.length,
    rows: parsedRows,
    firstRow,
    successRow,
  };
}

const state = {
  startedAt: nowIso(),
  lastPollAt: null,
  lastWorkerAt: null,
  lastError: null,
  shuttingDown: false,
  queueIndividual: [],
  queueRegular: [],
  queuedIds: new Set(),
  retryById: new Map(),
  resultOnlyById: new Map(),
  processing: null,
  tokenCursor: 0,
  tokenCache: {
    loadedAt: 0,
    items: [],
  },
  counters: {
    polledRows: 0,
    enqueuedRows: 0,
    processedClients: 0,
    successClients: 0,
    deferredClients: 0,
    failedClients: 0,
    totalAttempts: 0,
  },
};

function queueSize() {
  return state.queueIndividual.length + state.queueRegular.length;
}

function enqueueClient(client) {
  const id = Number(client.id);
  if (!Number.isFinite(id)) {
    return false;
  }

  if (state.processing && state.processing.id === id) {
    return false;
  }

  if (state.queuedIds.has(id)) {
    return false;
  }

  const retry = state.retryById.get(id);
  if (retry && retry.nextRetryAt > Date.now()) {
    return false;
  }

  const entry = {
    id,
    cliente_cpf: normalizeCpf(client.cliente_cpf),
    cliente_sexo: client.cliente_sexo,
    nascimento: client.nascimento,
    cliente_nome: client.cliente_nome,
    email: client.email,
    telefone: client.telefone,
    created_at: client.created_at,
    id_token_usado: client.id_token_usado,
    token_usado: client.token_usado,
    empresa: client.empresa,
    tipoConsulta: client.tipoConsulta,
    status_consulta_v8: client.status_consulta_v8,
  };

  if (!entry.cliente_cpf) {
    return false;
  }

  if (isIndividual(entry)) {
    state.queueIndividual.push(entry);
  } else {
    state.queueRegular.push(entry);
  }

  state.queuedIds.add(id);
  return true;
}

function dequeueClient() {
  let client = null;

  if (state.queueIndividual.length > 0) {
    client = state.queueIndividual.shift();
  } else if (state.queueRegular.length > 0) {
    client = state.queueRegular.shift();
  }

  if (client) {
    state.queuedIds.delete(client.id);
  }

  return client;
}

async function ensureSchema() {
  const pool = await getPool();
  const query = `
IF COL_LENGTH('dbo.clientes_v8', 'updated_at') IS NULL
BEGIN
  ALTER TABLE dbo.clientes_v8
  ADD updated_at DATETIME2 NULL;
END

IF COL_LENGTH('dbo.clientes_v8', 'mensagem') IS NULL
BEGIN
  ALTER TABLE dbo.clientes_v8
  ADD mensagem VARCHAR(MAX) NULL;
END
ELSE
BEGIN
  DECLARE @msgLen INT;
  SELECT @msgLen = CHARACTER_MAXIMUM_LENGTH
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'dbo'
    AND TABLE_NAME = 'clientes_v8'
    AND COLUMN_NAME = 'mensagem';

  IF @msgLen IS NOT NULL AND @msgLen <> -1
  BEGIN
    ALTER TABLE dbo.clientes_v8
    ALTER COLUMN mensagem VARCHAR(MAX) NULL;
  END
END

IF NOT EXISTS (
  SELECT 1
  FROM sys.sequences
  WHERE name = 'seq_clientes_v8_id'
    AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
  DECLARE @startId BIGINT;
  SELECT @startId = ISNULL(MAX(id), 0) + 1
  FROM dbo.clientes_v8;

  DECLARE @sqlSeq NVARCHAR(MAX) =
    N'CREATE SEQUENCE dbo.seq_clientes_v8_id AS BIGINT START WITH '
    + CAST(@startId AS NVARCHAR(30))
    + N' INCREMENT BY 1;';
  EXEC sp_executesql @sqlSeq;
END

IF NOT EXISTS (
  SELECT 1
  FROM sys.default_constraints dc
  JOIN sys.columns c
    ON c.object_id = dc.parent_object_id
   AND c.column_id = dc.parent_column_id
  WHERE dc.parent_object_id = OBJECT_ID('dbo.clientes_v8')
    AND c.name = 'id'
)
BEGIN
  ALTER TABLE dbo.clientes_v8
  ADD CONSTRAINT DF_clientes_v8_id
  DEFAULT (NEXT VALUE FOR dbo.seq_clientes_v8_id) FOR id;
END
`;

  await pool.request().query(query);
}

async function loadTokens(force = false) {
  const now = Date.now();
  if (
    !force &&
    state.tokenCache.items.length > 0 &&
    now - state.tokenCache.loadedAt <= config.worker.tokenCacheMs
  ) {
    return state.tokenCache.items;
  }

  const pool = await getPool();
  const query = `
;WITH x AS (
  SELECT
      t.*,
      ROW_NUMBER() OVER (PARTITION BY t.empresa ORDER BY t.updated_at DESC, t.id DESC) AS rn
  FROM dbo.tokens_v8 t
  WHERE LTRIM(RTRIM(COALESCE(t.origem_api, ''))) = 'V8 Digital'
    AND LTRIM(RTRIM(COALESCE(t.empresa, ''))) <> 'TK Financeira'
)
SELECT
    id,
    access_token,
    empresa,
    updated_at
FROM x
WHERE rn = 1
ORDER BY id ASC;
`;

  const result = await pool.request().query(query);
  const items = (result.recordset || []).filter((row) =>
    String(row.access_token || "").trim()
  );

  state.tokenCache.items = items;
  state.tokenCache.loadedAt = Date.now();

  if (items.length === 0) {
    throw new Error("Nenhum token disponivel em tokens_v8");
  }

  if (state.tokenCursor >= items.length) {
    state.tokenCursor = 0;
  }

  return items;
}

async function loadConsultDayById(idTokenUsado) {
  const parsedId = Number.parseInt(idTokenUsado, 10);
  if (!Number.isFinite(parsedId) || parsedId <= 0) {
    return null;
  }

  const pool = await getPool();
  const request = pool.request();
  request.input("id", sql.Int, parsedId);

  const query = `
SELECT TOP (1)
  id,
  email,
  senha,
  empresa,
  total,
  usado,
  restante,
  token,
  created_at,
  updated_at,
  DATEDIFF(SECOND, created_at, GETDATE()) AS age_seconds
FROM dbo.consult_day
WHERE id = @id;
`;

  const result = await request.query(query);
  return result.recordset?.[0] || null;
}

async function refreshConsultDayToken(entry) {
  const parsedId = Number.parseInt(entry?.id, 10);
  const email = String(entry?.email || "").trim();
  const senha = String(entry?.senha || "").trim();
  if (!Number.isFinite(parsedId) || parsedId <= 0) {
    throw new Error("consult_day sem id valido para refresh");
  }
  if (!email || !senha) {
    throw new Error(`consult_day ${parsedId} sem email/senha para refresh`);
  }

  const auth = await requestV8AccessToken(email, senha);
  const currentTotal = Number.parseInt(entry.total, 10);
  const total = Number.isFinite(currentTotal) && currentTotal > 0
    ? currentTotal
    : config.consultDay.defaultTotal;

  const pool = await getPool();
  const request = pool.request();
  request.input("id", sql.Int, parsedId);
  request.input("email", sql.VarChar(255), email);
  request.input("senha", sql.VarChar(255), senha);
  request.input("token", sql.VarChar(sql.MAX), auth.accessToken);
  request.input("total", sql.Int, total);

  const query = `
UPDATE dbo.consult_day
SET
  total = @total,
  usado = 0,
  restante = @total,
  token = @token,
  created_at = GETDATE(),
  updated_at = GETDATE()
OUTPUT inserted.id, inserted.total, inserted.usado, inserted.restante, inserted.created_at, inserted.updated_at
WHERE id = @id
  AND email = @email
  AND senha = @senha;
`;

  const result = await request.query(query);
  const row = result.recordset?.[0];
  if (!row) {
    throw new Error(`consult_day ${parsedId} nao encontrado para atualizar token`);
  }

  return {
    id: row.id,
    accessToken: auth.accessToken,
    total: row.total,
    usado: row.usado,
    restante: row.restante,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

async function resolveConsultDayTokenForClient(client) {
  const parsedId = Number.parseInt(client?.id_token_usado, 10);
  if (!Number.isFinite(parsedId) || parsedId <= 0) {
    return null;
  }

  const entry = await loadConsultDayById(parsedId);
  if (!entry) {
    return null;
  }

  const currentToken = String(entry.token || "").trim();
  const ageSeconds = Math.max(0, toSafeInt(entry.age_seconds, 0));
  const total = toSafeInt(entry.total, config.consultDay.defaultTotal);
  const usado = Math.max(0, toSafeInt(entry.usado, 0));
  const refreshAfterSeconds = Math.max(
    60,
    Math.floor(config.consultDay.refreshAfterMs / 1000)
  );
  const mustRefresh =
    !currentToken ||
    ageSeconds >= refreshAfterSeconds;

  if (!mustRefresh) {
    if (total > 0 && usado >= total) {
      const waitSeconds = Math.max(1, refreshAfterSeconds - ageSeconds);
      return {
        id: parsedId,
        blocked: true,
        waitMs: waitSeconds * 1000,
        total,
        usado,
        restante: Math.max(0, total - usado),
      };
    }

    return {
      id: parsedId,
      accessToken: currentToken,
      refreshed: false,
      total,
      usado,
      restante: Math.max(0, total - usado),
    };
  }

  const refreshed = await refreshConsultDayToken(entry);
  info("Token consult_day renovado", {
    consult_day_id: refreshed.id,
    total: refreshed.total,
    usado: refreshed.usado,
    restante: refreshed.restante,
  });
  return {
    id: refreshed.id,
    accessToken: refreshed.accessToken,
    refreshed: true,
    total: refreshed.total,
    usado: refreshed.usado,
    restante: refreshed.restante,
  };
}

function getNextToken(tokens) {
  if (!tokens || tokens.length === 0) {
    throw new Error("Lista de tokens vazia");
  }

  const index = state.tokenCursor % tokens.length;
  state.tokenCursor = (state.tokenCursor + 1) % tokens.length;
  return tokens[index];
}

function selectTokenForClient(tokens, client) {
  if (!Array.isArray(tokens) || tokens.length === 0) {
    throw new Error("Lista de tokens vazia");
  }

  const rowToken = String(client.token_usado || "").trim();
  if (rowToken) {
    const exact = tokens.find(
      (token) => String(token.access_token || "").trim() === rowToken
    );
    if (exact) {
      return exact;
    }

    const prefix = tokens.find((token) =>
      String(token.access_token || "").startsWith(rowToken)
    );
    if (prefix) {
      return prefix;
    }

    if (rowToken.length > 100) {
      return {
        id: "token_usado",
        empresa: client.empresa || "token_usado",
        access_token: rowToken,
      };
    }
  }

  const empresa = normalizeKey(client.empresa);
  if (empresa) {
    const empresaToken = tokens.find(
      (token) => normalizeKey(token.empresa) === empresa
    );
    if (empresaToken) {
      return empresaToken;
    }
  }

  return getNextToken(tokens);
}

async function pollPendingClients() {
  const pool = await getPool();
  const request = pool.request();
  request.input("limit", sql.Int, config.worker.fetchLimit);

  const query = `
SELECT TOP (@limit)
  id,
  cliente_cpf,
  cliente_sexo,
  nascimento,
  cliente_nome,
  email,
  telefone,
  created_at,
  id_token_usado,
  token_usado,
  empresa,
  tipoConsulta,
  status_consulta_v8
FROM dbo.clientes_v8 WITH (READPAST)
WHERE UPPER(ISNULL(LTRIM(RTRIM(status_consulta_v8)), '')) NOT IN (
  'WAITING_CONSENT',
  'AGUARDANDO CONSENTIMENTO',
  'SUCCESS',
  'SUCESSO',
  'FAILED',
  'FALHA',
  'REJECTED',
  'REJEITADO'
)
ORDER BY
  CASE WHEN ISNULL(tipoConsulta, '') = 'Individual' THEN 0 ELSE 1 END,
  id ASC;
`;

  const result = await request.query(query);
  const rows = result.recordset || [];

  state.lastPollAt = nowIso();
  state.counters.polledRows += rows.length;

  let added = 0;
  for (const row of rows) {
    if (enqueueClient(row)) {
      added += 1;
      state.counters.enqueuedRows += 1;
    }
  }

  if (added > 0) {
    info("Fila atualizada", {
      adicionados: added,
      fila_total: queueSize(),
      fila_individual: state.queueIndividual.length,
      fila_outros: state.queueRegular.length,
    });
  }
}

async function updateClient(id, payload) {
  const pool = await getPool();
  const request = pool.request();
  request.input("id", sql.BigInt, id);
  request.input("status", sql.VarChar(50), payload.statusConsulta);
  request.input("valor_liberado", sql.Decimal(18, 2), payload.valorLiberado);
  request.input("mensagem", sql.VarChar(sql.MAX), payload.mensagem);
  request.input("updated_at", sql.DateTime2, new Date());

  const query = `
UPDATE dbo.clientes_v8
SET
  status_consulta_v8 = COALESCE(@status, status_consulta_v8),
  valor_liberado = COALESCE(@valor_liberado, valor_liberado),
  mensagem = @mensagem,
  updated_at = COALESCE(@updated_at, updated_at)
WHERE id = @id;

SELECT @@ROWCOUNT AS rows_affected;
`;

  const result = await request.query(query);
  return result.recordset?.[0]?.rows_affected || 0;
}

async function consumeConsultDayQuotaById(idTokenUsado) {
  const parsedId = Number.parseInt(idTokenUsado, 10);
  if (!Number.isFinite(parsedId) || parsedId <= 0) {
    return { consumed: false, reason: "invalid_id_token_usado" };
  }

  const pool = await getPool();
  const request = pool.request();
  request.input("id", sql.Int, parsedId);

  const query = `
UPDATE dbo.consult_day
SET
  usado = COALESCE(usado, 0) + 1,
  restante = CASE
    WHEN total IS NULL THEN restante
    WHEN (COALESCE(usado, 0) + 1) >= total THEN 0
    ELSE total - (COALESCE(usado, 0) + 1)
  END,
  updated_at = GETDATE()
OUTPUT inserted.id, inserted.total, inserted.usado, inserted.restante, inserted.created_at, inserted.updated_at
WHERE id = @id
  AND (total IS NULL OR COALESCE(usado, 0) < total);
`;

  const result = await request.query(query);
  const row = result.recordset?.[0];
  if (row) {
    return {
      consumed: true,
      id: row.id,
      total: row.total,
      usado: row.usado,
      restante: row.restante,
      created_at: row.created_at,
      updated_at: row.updated_at,
    };
  }

  const statusReq = pool.request();
  statusReq.input("id", sql.Int, parsedId);
  const statusQuery = `
SELECT TOP (1)
  id,
  total,
  usado,
  restante,
  created_at,
  updated_at,
  DATEDIFF(SECOND, created_at, GETDATE()) AS age_seconds
FROM dbo.consult_day
WHERE id = @id;
`;
  const statusResult = await statusReq.query(statusQuery);
  const current = statusResult.recordset?.[0];
  if (!current) {
    return { consumed: false, reason: "consult_day_not_found", id: parsedId };
  }

  const total = toSafeInt(current.total, config.consultDay.defaultTotal);
  const usado = Math.max(0, toSafeInt(current.usado, 0));
  const refreshAfterSeconds = Math.max(
    60,
    Math.floor(config.consultDay.refreshAfterMs / 1000)
  );
  const ageSeconds = Math.max(0, toSafeInt(current.age_seconds, 0));
  const waitSeconds = Math.max(1, refreshAfterSeconds - ageSeconds);

  return {
    consumed: false,
    reason: "quota_exceeded",
    id: parsedId,
    total,
    usado,
    restante: Math.max(0, total - usado),
    waitMs: waitSeconds * 1000,
  };
}

async function duplicateClientRows(sourceId, rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return 0;
  }

  const pool = await getPool();
  let inserted = 0;

  for (const row of rows) {
    const request = pool.request();
    request.input("source_id", sql.BigInt, sourceId);
    request.input(
      "status",
      sql.VarChar(50),
      statusToDbValue(row?.statusRaw || row?.statusDb || "FAILED")
    );
    request.input("valor_liberado", sql.Decimal(18, 2), row?.marginValue ?? null);
    request.input(
      "mensagem",
      sql.VarChar(sql.MAX),
      descriptionToMessage(row?.description, null)
    );

    const query = `
INSERT INTO dbo.clientes_v8 (
  cliente_cpf,
  cliente_sexo,
  nascimento,
  cliente_nome,
  email,
  telefone,
  created_at,
  status_consulta_v8,
  valor_liberado,
  token_usado,
  empresa,
  id_user,
  tipoConsulta,
  mensagem,
  updated_at,
  id_token_usado
)
SELECT
  cliente_cpf,
  cliente_sexo,
  nascimento,
  cliente_nome,
  email,
  telefone,
  GETDATE(),
  @status,
  @valor_liberado,
  token_usado,
  empresa,
  id_user,
  tipoConsulta,
  @mensagem,
  GETDATE(),
  id_token_usado
FROM dbo.clientes_v8
WHERE id = @source_id;

SELECT @@ROWCOUNT AS rows_affected;
`;

    const result = await request.query(query);
    inserted += Number(result.recordset?.[0]?.rows_affected || 0);
  }

  return inserted;
}

async function deleteDuplicateSameConsultRows(cpf, nome, createdAt, keepId) {
  const normalizedCpf = normalizeCpf(cpf);
  const normalizedNome = String(nome || "").trim();
  if (!normalizedCpf || !normalizedNome || !createdAt) {
    return 0;
  }

  const pool = await getPool();
  const request = pool.request();
  request.input("cpf", sql.VarChar(11), normalizedCpf);
  request.input("nome", sql.VarChar(255), normalizedNome);
  request.input("created_at", sql.DateTime2, new Date(createdAt));
  request.input("keep_id", sql.BigInt, keepId);

  const query = `
;WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY
        RIGHT(
          REPLICATE('0', 11) +
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(cliente_cpf, ''))), '.', ''), '-', ''), '/', ''), ' ', ''), CHAR(9), ''),
          11
        ),
        LTRIM(RTRIM(ISNULL(cliente_nome, ''))),
        created_at
      ORDER BY
        CASE WHEN id = @keep_id THEN 0 ELSE 1 END,
        updated_at DESC,
        id DESC
    ) AS rn
  FROM dbo.clientes_v8
  WHERE RIGHT(
      REPLICATE('0', 11) +
      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(cliente_cpf, ''))), '.', ''), '-', ''), '/', ''), ' ', ''), CHAR(9), ''),
      11
    ) = @cpf
    AND LTRIM(RTRIM(ISNULL(cliente_nome, ''))) = @nome
    AND created_at = @created_at
)
DELETE FROM dbo.clientes_v8
WHERE id IN (
  SELECT id
  FROM ranked
  WHERE rn > 1
);

SELECT @@ROWCOUNT AS rows_affected;
`;

  const result = await request.query(query);
  return Number(result.recordset?.[0]?.rows_affected || 0);
}

function pushResultOnlyRetryTempLog(entry) {
  try {
    const logPath = config.worker.resultOnlyRetryLogPath;
    fs.mkdirSync(path.dirname(logPath), { recursive: true });
    fs.appendFileSync(
      logPath,
      `${JSON.stringify({ ...entry, logged_at: nowIso() })}\n`,
      "utf8"
    );
  } catch (err) {
    error("Falha ao registrar log temporario de retry", {
      erro: err?.message || String(err),
    });
  }
}

async function processClient(client) {
  state.processing = {
    id: client.id,
    cpf: client.cliente_cpf,
    tipoConsulta: client.tipoConsulta,
    startedAt: nowIso(),
  };
  state.lastWorkerAt = nowIso();

  let lastResponsePayload = null;
  let lastErrorMessage = null;
  let lastApiStatus = null;
  let lastParsedPayload = null;
  let marginValue = null;
  let success = false;
  let attemptsExecuted = 0;
  let stopAttemptsWithoutRetry = false;
  let runOnlyLastApi = state.resultOnlyById.has(client.id);
  let shouldParkForResultOnly = false;
  let consultDayLimitWaitMs = null;
  let lastConsultDayQuota = null;
  let consultDayToken = null;
  let tokens = [];

  try {
    try {
      consultDayToken = await resolveConsultDayTokenForClient(client);
    } catch (tokenErr) {
      error("Falha ao resolver token do consult_day; usando fallback", {
        id: client.id,
        consult_day_id: client.id_token_usado,
        erro: tokenErr?.message || String(tokenErr),
      });
    }
    if (consultDayToken?.blocked) {
      consultDayLimitWaitMs = Math.max(
        1000,
        toSafeInt(consultDayToken.waitMs, config.consultDay.refreshAfterMs)
      );
      stopAttemptsWithoutRetry = true;
      lastErrorMessage = "Limite de consultas/hora atingido para token consult_day";
    }
    try {
      tokens = await loadTokens();
    } catch (loadErr) {
      if (!consultDayToken || !consultDayToken.accessToken) {
        throw loadErr;
      }
      error("Falha ao carregar tokens_v8; seguindo com token consult_day", {
        id: client.id,
        erro: loadErr?.message || String(loadErr),
      });
    }

    for (let attempt = 1; attempt <= config.worker.maxAttempts; attempt += 1) {
      if (stopAttemptsWithoutRetry) {
        break;
      }
      attemptsExecuted = attempt;
      state.counters.totalAttempts += 1;
      let retryThisCycle = false;

      const token =
        consultDayToken && consultDayToken.accessToken
          ? {
              id: `consult_day_${consultDayToken.id}`,
              empresa: client.empresa || "consult_day",
              access_token: consultDayToken.accessToken,
            }
          : selectTokenForClient(tokens, client);
      const accessToken = String(token.access_token || "").trim();

      try {
        if (!runOnlyLastApi) {
          const createResp = await createConsult(accessToken, client);
          const createStatus = Number(createResp.status || 0);
          const createData = createResp.data || {};
          const consultId = createData.id || createData.consultId || null;
          const isAlreadyActiveConsult =
            createStatus === 400 && isConsultAlreadyExistsError(createData);

          // Conta no consult_day somente quando a consulta parte do zero.
          // No fluxo "somente GET final" (consulta ja ativa), nao consome cota.
          if (consultDayToken?.id && !isAlreadyActiveConsult) {
            const quota = await consumeConsultDayQuotaById(consultDayToken.id);
            if (!quota.consumed) {
              if (quota.reason === "quota_exceeded") {
                consultDayLimitWaitMs = Math.max(
                  1000,
                  toSafeInt(quota.waitMs, config.consultDay.refreshAfterMs)
                );
                lastErrorMessage =
                  "Limite de 250 consultas por hora atingido; aguardando nova janela";
                shouldParkForResultOnly = true;
                stopAttemptsWithoutRetry = true;
                break;
              }
              throw new Error(`Falha ao consumir cota consult_day: ${quota.reason}`);
            }
            lastConsultDayQuota = quota;
          }

          if (consultId) {
            await sleep(config.worker.waitBetweenApisMs);
          const authResp = await authorizeConsult(accessToken, consultId);
          if (Number(authResp.status || 0) >= 400) {
            lastApiStatus = normalizeStatusForDb(`HTTP_${authResp.status}`, lastApiStatus || "Pendente");
            const authMessage = extractApiErrorMessage(authResp.data);
            if (authMessage) {
              lastErrorMessage = authMessage;
            }
            lastResponsePayload = {
              step: "authorize",
              status: authResp.status,
              data: authResp.data,
            };
          }
        } else if (isAlreadyActiveConsult) {
          runOnlyLastApi = true;
          shouldParkForResultOnly = true;
          lastApiStatus = normalizeStatusForDb(
            "WAITING_CONSULT",
            lastApiStatus || "Pendente"
          );
          const createMessage = extractApiErrorMessage(createData);
          if (createMessage) {
            lastErrorMessage = createMessage;
          }
          lastResponsePayload = {
            step: "consult_create_already_exists",
            status: createStatus,
            data: createData,
          };
          info("Consulta ja ativa na V8; seguindo somente com API final", {
            id: client.id,
            cpf: client.cliente_cpf,
            tipoConsulta: client.tipoConsulta || "",
          });
        } else if (createStatus >= 400) {
          lastApiStatus = normalizeStatusForDb(`HTTP_${createStatus}`, lastApiStatus || "Pendente");
          const createMessage = extractApiErrorMessage(createResp.data);
          if (createMessage) {
            lastErrorMessage = createMessage;
          }
          lastResponsePayload = {
            step: "consult_create",
            status: createStatus,
            data: createResp.data,
          };
          }
        }

        await sleep(config.worker.waitBetweenApisMs);
        const resultResp = await getConsultResult(accessToken, client.cliente_cpf);
        const resultPayload = resultResp.data || {};
        lastResponsePayload = resultPayload;
        const resultStatus = Number(resultResp.status || 0);
        if (resultStatus >= 400) {
          lastApiStatus = normalizeStatusForDb(`HTTP_${resultStatus}`, lastApiStatus || "Pendente");
          const resultMessage = extractApiErrorMessage(resultPayload);
          if (resultMessage) {
            lastErrorMessage = resultMessage;
          }
        }

        const parsed = parseConsultPayload(resultPayload);
        lastParsedPayload = parsed;
        if (parsed.statusText) {
          lastApiStatus = normalizeStatusForDb(parsed.statusText, lastApiStatus || "Pendente");
        }
        if (parsed.marginValue !== null) {
          marginValue = parsed.marginValue;
        }

        if (parsed.hasSuccess) {
          success = true;
          stopAttemptsWithoutRetry = true;
          state.resultOnlyById.delete(client.id);
          break;
        }

        if (runOnlyLastApi && parsed.totalRows === 0) {
          lastApiStatus = normalizeStatusForDb(
            "WAITING_CONSULT",
            lastApiStatus || "WAITING_CONSULT"
          );
          if (!lastErrorMessage) {
            lastErrorMessage =
              "Consulta ativa identificada; aguardando retorno da API final";
          }
          if (attempt < config.worker.maxAttempts) {
            retryThisCycle = true;
          } else {
            shouldParkForResultOnly = true;
            stopAttemptsWithoutRetry = true;
          }
        } else if (isResultOnlyRetryStatus(parsed.statusText)) {
          runOnlyLastApi = true;
          state.resultOnlyById.set(client.id, {
            status: normalizeStatusToken(parsed.statusText),
            updatedAt: Date.now(),
          });
          lastErrorMessage =
            parsed.descriptionText ||
            parsed.statusText ||
            `Status intermediario na tentativa ${attempt}`;
          if (attempt < config.worker.maxAttempts) {
            retryThisCycle = true;
          } else {
            shouldParkForResultOnly = true;
            stopAttemptsWithoutRetry = true;
          }
        } else if (isNoRetryFinalStatus(parsed.statusText)) {
          stopAttemptsWithoutRetry = true;
          state.resultOnlyById.delete(client.id);
        } else {
          stopAttemptsWithoutRetry = true;
        }

        if (!lastErrorMessage) {
          lastErrorMessage =
            parsed.descriptionText ||
            parsed.statusText ||
            `Sem SUCCESS apos ${attempt} tentativa(s)`;
        }
      } catch (err) {
        lastErrorMessage = err?.message || String(err);
        lastResponsePayload = {
          step: "exception",
          attempt,
          message: lastErrorMessage,
        };
        if (runOnlyLastApi && attempt < config.worker.maxAttempts) {
          retryThisCycle = true;
        } else {
          stopAttemptsWithoutRetry = true;
        }
      }

      if (stopAttemptsWithoutRetry) {
        break;
      }

      if (retryThisCycle && attempt < config.worker.maxAttempts) {
        await sleep(config.worker.retryBetweenAttemptsMs);
      } else {
        break;
      }
    }

    const parsedRows = Array.isArray(lastParsedPayload?.rows)
      ? lastParsedPayload.rows
      : [];
    const primaryRow = parsedRows[0] || null;
    const duplicateRows = parsedRows.length > 1 ? parsedRows.slice(1) : [];
    const firstRowWithDescription =
      parsedRows.find(
        (row) =>
          row &&
          row.description !== undefined &&
          row.description !== null &&
          String(row.description).trim() !== ""
      ) || null;
    const primaryDescription = primaryRow?.description ?? firstRowWithDescription?.description ?? null;
    const primaryMessage = descriptionToMessage(primaryDescription, lastErrorMessage);
    const primaryMargin =
      primaryRow && primaryRow.marginValue !== null
        ? primaryRow.marginValue
        : marginValue;

    if (success) {
      await updateClient(client.id, {
        statusConsulta: "Sucesso",
        valorLiberado: primaryMargin,
        mensagem: primaryMessage,
      });
      const duplicatedCount = await duplicateClientRows(client.id, duplicateRows);
      const removedDuplicates = await deleteDuplicateSameConsultRows(
        client.cliente_cpf,
        client.cliente_nome,
        client.created_at,
        client.id
      );
      state.counters.successClients += 1;
      state.retryById.delete(client.id);
      state.resultOnlyById.delete(client.id);

      info("Cliente concluido com SUCCESS", {
        id: client.id,
        cpf: client.cliente_cpf,
        tipoConsulta: client.tipoConsulta || "",
        consult_day_id: lastConsultDayQuota?.id,
        consult_day_usado: lastConsultDayQuota?.usado,
        consult_day_restante: lastConsultDayQuota?.restante,
        tentativas_executadas: attemptsExecuted,
        duplicados: duplicatedCount,
        duplicados_removidos_cpf: removedDuplicates,
      });
    } else {
      const finalStatus = statusToDbValue(
        primaryRow?.statusRaw || lastApiStatus || "Pendente"
      );
      await updateClient(client.id, {
        statusConsulta: finalStatus,
        valorLiberado: primaryMargin,
        mensagem: primaryMessage,
      });
      const duplicatedCount = await duplicateClientRows(client.id, duplicateRows);
      const removedDuplicates = await deleteDuplicateSameConsultRows(
        client.cliente_cpf,
        client.cliente_nome,
        client.created_at,
        client.id
      );
      if (isResultOnlyRetryStatus(finalStatus)) {
        const delayMs =
          consultDayLimitWaitMs || config.worker.retryCooldownMs;
        state.counters.deferredClients += 1;
        state.retryById.set(client.id, {
          nextRetryAt: Date.now() + delayMs,
        });
        state.resultOnlyById.set(client.id, {
          status: normalizeStatusToken(finalStatus),
          updatedAt: Date.now(),
        });
        if (shouldParkForResultOnly) {
          pushResultOnlyRetryTempLog({
            id: client.id,
            cpf: client.cliente_cpf,
            status: finalStatus,
            mensagem: primaryMessage,
            modo: "ultima_api",
          });
        }
        info("Cliente movido para fila temporaria da ultima API", {
          id: client.id,
          cpf: client.cliente_cpf,
          tipoConsulta: client.tipoConsulta || "",
          status_final: finalStatus,
          tentativas_executadas: attemptsExecuted,
          duplicados: duplicatedCount,
          duplicados_removidos_cpf: removedDuplicates,
          result_only: true,
          retry_em_ms: delayMs,
        });
      } else if (isNoRetryFinalStatus(finalStatus)) {
        state.retryById.delete(client.id);
        state.resultOnlyById.delete(client.id);
        info("Cliente finalizado sem retry (status final)", {
          id: client.id,
          cpf: client.cliente_cpf,
          tipoConsulta: client.tipoConsulta || "",
          status_final: finalStatus,
          tentativas_executadas: attemptsExecuted,
          duplicados: duplicatedCount,
          duplicados_removidos_cpf: removedDuplicates,
        });
      } else {
        const delayMs =
          consultDayLimitWaitMs || config.worker.retryCooldownMs;
        state.counters.deferredClients += 1;
        state.retryById.set(client.id, {
          nextRetryAt: Date.now() + delayMs,
        });
        if (runOnlyLastApi || shouldParkForResultOnly) {
          state.resultOnlyById.set(client.id, {
            status: normalizeStatusToken(lastApiStatus || finalStatus),
            updatedAt: Date.now(),
          });
          if (shouldParkForResultOnly) {
            pushResultOnlyRetryTempLog({
              id: client.id,
              cpf: client.cliente_cpf,
              status: finalStatus,
              mensagem: primaryMessage,
              modo: "ultima_api_quota",
            });
          }
        } else {
          state.resultOnlyById.delete(client.id);
        }

        info("Cliente sem SUCCESS; retornado para fila", {
          id: client.id,
          cpf: client.cliente_cpf,
          tipoConsulta: client.tipoConsulta || "",
          status_final: finalStatus,
          tentativas_executadas: attemptsExecuted,
          duplicados: duplicatedCount,
          duplicados_removidos_cpf: removedDuplicates,
          retry_em_ms: delayMs,
          result_only: runOnlyLastApi || shouldParkForResultOnly,
        });
      }
    }

    state.counters.processedClients += 1;
  } catch (err) {
    const message = err?.message || String(err);
    state.lastError = {
      at: nowIso(),
      message,
    };
    state.counters.failedClients += 1;

    error("Falha ao processar cliente", {
      id: client.id,
      cpf: client.cliente_cpf,
      erro: message,
    });

    try {
      const fallbackStatus = statusToDbValue(lastApiStatus || "FAILED");
      await updateClient(client.id, {
        statusConsulta: fallbackStatus,
        valorLiberado: null,
        mensagem: descriptionToMessage(lastParsedPayload?.firstRow?.description, message),
      });
    } catch (dbErr) {
      error("Falha ao atualizar cliente apos erro", {
        id: client.id,
        erro_db: dbErr?.message || String(dbErr),
      });
    }

    if (isResultOnlyRetryStatus(lastApiStatus)) {
      const delayMs =
        consultDayLimitWaitMs || config.worker.retryCooldownMs;
      state.retryById.set(client.id, {
        nextRetryAt: Date.now() + delayMs,
      });
      state.resultOnlyById.set(client.id, {
        status: normalizeStatusToken(lastApiStatus),
        updatedAt: Date.now(),
      });
      pushResultOnlyRetryTempLog({
        id: client.id,
        cpf: client.cliente_cpf,
        status: statusToDbValue(lastApiStatus),
        mensagem: message,
        modo: "ultima_api_erro",
      });
    } else if (isNoRetryFinalStatus(lastApiStatus)) {
      state.retryById.delete(client.id);
      state.resultOnlyById.delete(client.id);
    } else {
      const delayMs =
        consultDayLimitWaitMs || config.worker.retryCooldownMs;
      state.retryById.set(client.id, {
        nextRetryAt: Date.now() + delayMs,
      });
      if (runOnlyLastApi || shouldParkForResultOnly) {
        state.resultOnlyById.set(client.id, {
          status: normalizeStatusToken(lastApiStatus || "PENDENTE"),
          updatedAt: Date.now(),
        });
      } else {
        state.resultOnlyById.delete(client.id);
      }
    }
  } finally {
    state.processing = null;
    if (config.worker.waitBetweenClientsMs > 0) {
      await sleep(config.worker.waitBetweenClientsMs);
    }
  }
}

async function workerLoop() {
  while (!state.shuttingDown) {
    const next = dequeueClient();
    if (!next) {
      await sleep(400);
      continue;
    }

    await processClient(next);
  }
}

async function pollLoop() {
  while (!state.shuttingDown) {
    try {
      await pollPendingClients();
    } catch (err) {
      const message = err?.message || String(err);
      state.lastError = {
        at: nowIso(),
        message,
      };
      error("Falha no poll de pendentes", { erro: message });
    }

    await sleep(config.worker.pollIntervalMs);
  }
}

function countPendingRetries() {
  let pending = 0;
  const now = Date.now();
  for (const item of state.retryById.values()) {
    if (item.nextRetryAt > now) {
      pending += 1;
    }
  }
  return pending;
}

function countResultOnlyTracked() {
  return state.resultOnlyById.size;
}

function buildStatus() {
  return {
    ok: true,
    started_at: state.startedAt,
    server_time: nowIso(),
    poll_interval_ms: config.worker.pollIntervalMs,
    queue: {
      total: queueSize(),
      individual: state.queueIndividual.length,
      outros: state.queueRegular.length,
      retries_em_espera: countPendingRetries(),
      ultima_api_em_espera: countResultOnlyTracked(),
    },
    processing: state.processing,
    counters: state.counters,
    last_poll_at: state.lastPollAt,
    last_worker_at: state.lastWorkerAt,
    last_error: state.lastError,
  };
}

let server;
let pollPromise;
let workerPromise;

async function bootstrap() {
  await getPool();
  await ensureSchema();
  await loadTokens(true);

  const app = express();
  app.use(express.json({ limit: "1mb" }));

  app.get("/health", (_req, res) => {
    res.status(200).json({ ok: true, service: "api-consultaV8-queue" });
  });

  app.get("/api/status", (_req, res) => {
    res.status(200).json(buildStatus());
  });

  app.post("/api/poll", async (_req, res) => {
    try {
      await pollPendingClients();
      return res.status(200).json({ ok: true, queue: buildStatus().queue });
    } catch (err) {
      return res.status(500).json({ ok: false, error: err?.message || String(err) });
    }
  });

  server = app.listen(config.server.port, config.server.host, () => {
    info("API fila V8 iniciada", {
      host: config.server.host,
      port: config.server.port,
      poll_interval_ms: config.worker.pollIntervalMs,
      max_attempts: config.worker.maxAttempts,
    });
  });

  await pollPendingClients();
  pollPromise = pollLoop();
  workerPromise = workerLoop();
}

async function shutdown(signal) {
  if (state.shuttingDown) {
    return;
  }

  state.shuttingDown = true;
  info("Encerrando aplicacao", { signal });

  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }

  try {
    await Promise.race([
      Promise.allSettled([pollPromise, workerPromise]),
      sleep(5000),
    ]);
  } catch (_err) {
    // noop
  }

  await closePool();
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

bootstrap().catch(async (err) => {
  error("Falha ao iniciar aplicacao", { erro: err?.message || String(err) });
  await closePool();
  process.exit(1);
});
