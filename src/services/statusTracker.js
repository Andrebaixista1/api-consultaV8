function pad2(value) {
  return String(value).padStart(2, "0");
}

function pad4(value) {
  return String(value).padStart(4, "0");
}

function formatDate(date) {
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(
    date.getDate()
  )}`;
}

function formatDateTime(date) {
  return `${formatDate(date)} ${pad2(date.getHours())}:${pad2(
    date.getMinutes()
  )}:${pad2(date.getSeconds())}`;
}

function formatDuration(ms) {
  const safeMs = Math.max(0, Number(ms) || 0);
  const totalSeconds = Math.floor(safeMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const millis = safeMs % 1000;
  return `${pad2(hours)}:${pad2(minutes)}:${pad2(seconds)}:${pad4(millis)}`;
}

class StatusTracker {
  constructor(options = {}) {
    this.host = options.host || "0.0.0.0";
    this.port = String(options.port || "");
    this.currentCycle = null;
    this.lastCycle = null;
    this.apiErrors = {
      totalCount: 0,
      lastCycleCount: 0,
      last: null,
      hadErrorsInCurrentCycle: false,
    };
    this.dbErrors = {
      totalCount: 0,
      lastCycleCount: 0,
      last: null,
      hadErrorsInCurrentCycle: false,
    };
    this.lastError = null;
  }

  setServerStatus(host, port) {
    if (host) {
      this.host = host;
    }
    if (port !== undefined && port !== null) {
      this.port = String(port);
    }
  }

  startCycle(source) {
    this.currentCycle = {
      source: source || "unknown",
      startedAt: new Date(),
      apiErrorsCount: 0,
      dbErrorsCount: 0,
      totalWindows: 0,
    };

    this.apiErrors.lastCycleCount = 0;
    this.apiErrors.hadErrorsInCurrentCycle = false;
    this.dbErrors.lastCycleCount = 0;
    this.dbErrors.hadErrorsInCurrentCycle = false;
  }

  incrementWindow() {
    if (!this.currentCycle) {
      return;
    }

    this.currentCycle.totalWindows += 1;
  }

  recordApiError({ route, status, message }) {
    const now = new Date();
    const item = {
      at: formatDateTime(now),
      route: route || "unknown",
      status: Number.isInteger(status) ? status : 500,
      message: String(message || "Erro de API"),
      _atMs: now.getTime(),
    };

    this.apiErrors.totalCount += 1;
    this.apiErrors.last = item;
    this.apiErrors.hadErrorsInCurrentCycle = !!this.currentCycle;
    if (this.currentCycle) {
      this.currentCycle.apiErrorsCount += 1;
    }

    this.lastError = {
      at: item.at,
      message: item.message,
      _atMs: item._atMs,
    };
  }

  recordDbError({ route, status, message }) {
    const now = new Date();
    const item = {
      at: formatDateTime(now),
      route: route || "db",
      status: Number.isInteger(status) ? status : 500,
      message: String(message || "Erro de banco"),
      _atMs: now.getTime(),
    };

    this.dbErrors.totalCount += 1;
    this.dbErrors.last = item;
    this.dbErrors.hadErrorsInCurrentCycle = !!this.currentCycle;
    if (this.currentCycle) {
      this.currentCycle.dbErrorsCount += 1;
    }

    this.lastError = {
      at: item.at,
      message: item.message,
      _atMs: item._atMs,
    };
  }

  completeCycle() {
    if (!this.currentCycle) {
      return;
    }

    const completedAt = new Date();
    const durationMs = completedAt.getTime() - this.currentCycle.startedAt.getTime();

    const apiErrorsInCycle = this.currentCycle.apiErrorsCount;
    const dbErrorsInCycle = this.currentCycle.dbErrorsCount;

    this.lastCycle = {
      started_at: formatDateTime(this.currentCycle.startedAt),
      completed_at: formatDateTime(completedAt),
      period: {
        start: formatDate(this.currentCycle.startedAt),
        end: formatDate(completedAt),
      },
      duration_ms: durationMs,
      duration_hhmmssmmmm: formatDuration(durationMs),
      api_errors: apiErrorsInCycle,
      db_errors: dbErrorsInCycle,
      total_windows: this.currentCycle.totalWindows,
      had_api_errors: apiErrorsInCycle > 0,
      had_db_errors: dbErrorsInCycle > 0,
    };

    this.apiErrors.lastCycleCount = apiErrorsInCycle;
    this.dbErrors.lastCycleCount = dbErrorsInCycle;
    this.apiErrors.hadErrorsInCurrentCycle = false;
    this.dbErrors.hadErrorsInCurrentCycle = false;
    this.currentCycle = null;
  }

  buildCurrentCycle() {
    if (!this.currentCycle) {
      return null;
    }

    const now = new Date();
    const durationMs = now.getTime() - this.currentCycle.startedAt.getTime();

    return {
      started_at: formatDateTime(this.currentCycle.startedAt),
      period: {
        start: formatDate(this.currentCycle.startedAt),
        end: formatDate(now),
      },
      duration_ms: durationMs,
      duration_hhmmssmmmm: formatDuration(durationMs),
      api_errors: this.currentCycle.apiErrorsCount,
      db_errors: this.currentCycle.dbErrorsCount,
      total_windows: this.currentCycle.totalWindows,
      had_api_errors: this.currentCycle.apiErrorsCount > 0,
      had_db_errors: this.currentCycle.dbErrorsCount > 0,
    };
  }

  sanitizeErrorItem(item) {
    if (!item) {
      return null;
    }

    return {
      at: item.at,
      route: item.route,
      status: item.status,
      message: item.message,
    };
  }

  sanitizeLastError(item) {
    if (!item) {
      return null;
    }

    return {
      at: item.at,
      message: item.message,
    };
  }

  getStatus() {
    return {
      current_cycle: this.buildCurrentCycle(),
      last_cycle: this.lastCycle,
      api_errors: {
        total_count: this.apiErrors.totalCount,
        last_cycle_count: this.apiErrors.lastCycleCount,
        last: this.sanitizeErrorItem(this.apiErrors.last),
        had_errors_in_current_cycle: this.apiErrors.hadErrorsInCurrentCycle,
      },
      db_errors: {
        total_count: this.dbErrors.totalCount,
        last_cycle_count: this.dbErrors.lastCycleCount,
        last: this.sanitizeErrorItem(this.dbErrors.last),
        had_errors_in_current_cycle: this.dbErrors.hadErrorsInCurrentCycle,
      },
      last_error: this.sanitizeLastError(this.lastError),
      server_time: formatDateTime(new Date()),
      status_server: {
        host: this.host,
        port: this.port,
      },
    };
  }
}

module.exports = {
  StatusTracker,
  formatDateTime,
};
