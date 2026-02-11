function nowIso() {
  return new Date().toISOString();
}

function normalizeSpaces(value) {
  return String(value).replace(/\s+/g, " ").trim();
}

function shorten(value, max = 140) {
  if (value.length <= max) {
    return value;
  }

  return `${value.slice(0, max - 3)}...`;
}

function formatValue(value) {
  if (value === undefined) {
    return "undefined";
  }
  if (value === null) {
    return "null";
  }

  if (typeof value === "string") {
    return shorten(normalizeSpaces(value));
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return "[]";
    }

    if (value.length <= 4) {
      const items = value.map((item) => formatValue(item)).join(", ");
      return `[${items}]`;
    }

    return `Array(${value.length})`;
  }

  const json = JSON.stringify(value);
  if (!json) {
    return "Object";
  }

  return shorten(normalizeSpaces(json));
}

function formatMeta(meta = {}) {
  const entries = Object.entries(meta);
  if (entries.length === 0) {
    return "";
  }

  const parts = entries.map(([key, value]) => `${key}=${formatValue(value)}`);
  return ` | ${parts.join(" | ")}`;
}

function log(level, message, meta = {}) {
  const line = `[${nowIso()}] ${level.toUpperCase()} ${message}${formatMeta(meta)}`;
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

module.exports = {
  info,
  error,
};
