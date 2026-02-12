const { getPool } = require("../db");
const { info, error } = require("../logger");
const {
  ensureDescricaoColumn,
  ensureTokenUsadoColumn,
  getPendingClientsBatch,
  updateClientByCpf,
} = require("../repositories/clientRepository");
const {
  getLatestTokensByEmpresa,
} = require("../repositories/tokenRepository");

const MAX_CONSULTAS_HORA_POR_TOKEN = 250;
const HOUR_MS = 60 * 60 * 1000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function hasValue(value) {
  return value !== undefined && value !== null && String(value).trim() !== "";
}

function hasValidClientData(client) {
  return (
    hasValue(client.cliente_cpf) &&
    hasValue(client.cliente_sexo) &&
    hasValue(client.nascimento) &&
    hasValue(client.cliente_nome)
  );
}

function normalizeCpf(cpfValue) {
  const digits = String(cpfValue || "").replace(/\D/g, "");

  if (!digits || digits.length > 11) {
    return null;
  }

  return digits.padStart(11, "0");
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

function mapStatus(status) {
  if (!hasValue(status)) {
    return null;
  }

  const upper = String(status).toUpperCase();
  const map = {
    CONSENT_APPROVED: "Consentimento Aprovado",
    WAITING_CONSENT: "Aguardando Consentimento",
    WAITING_CONSULT: "Aguardando Consulta",
    WAITING_CREDIT_ANALYSIS: "Aguardando Analise Credito",
    FAILED: "Falha",
    REJECTED: "Rejeitado",
    SUCCESS: "Sucesso",
  };

  return map[upper] || String(status);
}

function cleanDescription(description) {
  if (!hasValue(description)) {
    return null;
  }

  const value = String(description).trim();
  return value || null;
}

function buildProgressBar(current, total, width = 20) {
  const safeTotal = total > 0 ? total : 1;
  const clamped = Math.min(Math.max(current, 0), safeTotal);
  const ratio = clamped / safeTotal;
  const filled = Math.round(ratio * width);
  const empty = width - filled;
  const percent = Math.round(ratio * 100);

  return `[${"#".repeat(filled)}${"-".repeat(empty)}] ${clamped}/${safeTotal} (${percent}%)`;
}

function formatDurationHhMmSs(ms) {
  const totalSeconds = Math.max(0, Math.floor((Number(ms) || 0) / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(
    2,
    "0"
  )}:${String(seconds).padStart(2, "0")}`;
}

function progressPercent(current, total) {
  const safeTotal = total > 0 ? total : 1;
  const clamped = Math.min(Math.max(current, 0), safeTotal);
  return Math.round((clamped / safeTotal) * 100);
}

function compactProgressText(current, total, width = 22) {
  if (total <= 0) {
    return `[${"-".repeat(width)}] 0/0 (100%)`;
  }

  return buildProgressBar(current, total, width);
}

function estimateEtaText(current, total, startedAtMs) {
  if (total <= 0) {
    return "00:00:00";
  }

  const safeCurrent = Math.min(Math.max(current, 0), total);
  if (safeCurrent <= 0) {
    return "--:--:--";
  }

  const elapsedMs = Math.max(0, Date.now() - startedAtMs);
  const remaining = total - safeCurrent;
  if (remaining <= 0) {
    return "00:00:00";
  }

  const avgMsPerClient = elapsedMs / safeCurrent;
  const remainingMs = Math.max(0, Math.round(avgMsPerClient * remaining));
  return formatDurationHhMmSs(remainingMs);
}

function shouldLogProgress(lastPercent, currentPercent, current, total) {
  if (total <= 0) {
    return current === 0;
  }
  if (current === 0 || current === total) {
    return true;
  }
  if (lastPercent < 0) {
    return true;
  }
  return currentPercent - lastPercent >= 5;
}

function splitClientsAcrossTokens(clients, totalTokens, maxPerToken) {
  const safeTokens = Math.max(0, Number.parseInt(totalTokens, 10) || 0);
  const safePerToken = Math.max(0, Number.parseInt(maxPerToken, 10) || 0);
  const batches = Array.from({ length: safeTokens }, () => []);

  if (!Array.isArray(clients) || safeTokens <= 0 || safePerToken <= 0) {
    return batches;
  }

  const totalCapacity = safeTokens * safePerToken;
  const clientsToDistribute = clients.slice(0, totalCapacity);

  for (const [index, client] of clientsToDistribute.entries()) {
    const tokenIndex = index % safeTokens;
    batches[tokenIndex].push(client);
  }

  return batches;
}

function buildTokenSummary(tokenPosicao, token) {
  return {
    ok: true,
    message: null,
    tokenPosicao,
    tokenId: token.id,
    empresa: token.empresa,
    totalClientesSelecionados: 0,
    totalProcessados: 0,
    totalIgnoradosDadosInvalidos: 0,
    totalConsultasCriadas: 0,
    totalConsultasAtivas400: 0,
    totalErrosAutorizar: 0,
    totalResultadosEncontrados: 0,
    totalResultadosSemDados: 0,
    totalLinhasAtualizadas: 0,
    totalErrosApi: 0,
    totalErrosDb: 0,
    durationMs: 0,
  };
}

class ConsignmentJobService {
  constructor(options) {
    this.dbConfig = options.dbConfig;
    this.jobConfig = options.jobConfig;
    this.v8Client = options.v8Client;
    this.statusTracker = options.statusTracker || null;
    this.isRunning = false;
    this.tokenLastWindowStartedAt = new Map();
    this.lastRunSnapshot = {
      updated_at: null,
      last_cycle: null,
      tokens: [],
      linhas_resumo: [],
    };
  }

  recordApiError(route, status, message) {
    if (!this.statusTracker) {
      return;
    }

    this.statusTracker.recordApiError({
      route,
      status,
      message,
    });
  }

  recordDbError(route, status, message) {
    if (!this.statusTracker) {
      return;
    }

    this.statusTracker.recordDbError({
      route,
      status,
      message,
    });
  }

  getTokenRateKey(token) {
    if (hasValue(token.empresa)) {
      return `empresa:${String(token.empresa).trim()}`;
    }
    return `token:${String(token.id || "sem_id")}`;
  }

  async waitForTokenHourlyWindow(token, tokenPosicao, totalTokens) {
    const tokenKey = this.getTokenRateKey(token);
    const now = Date.now();
    const lastWindowStartedAt = this.tokenLastWindowStartedAt.get(tokenKey);

    if (lastWindowStartedAt && now - lastWindowStartedAt < HOUR_MS) {
      const waitMs = HOUR_MS - (now - lastWindowStartedAt);
      info(
        `Token ${tokenPosicao}/${totalTokens} aguardando ${formatDurationHhMmSs(
          waitMs
        )} para respeitar limite de ${MAX_CONSULTAS_HORA_POR_TOKEN} consultas/h`
      );
      await sleep(waitMs);
    }

    this.tokenLastWindowStartedAt.set(tokenKey, Date.now());
  }

  mergeTokenSummary(summary, tokenSummary) {
    summary.totalClientesSelecionados += tokenSummary.totalClientesSelecionados;
    summary.totalProcessados += tokenSummary.totalProcessados;
    summary.totalIgnoradosDadosInvalidos += tokenSummary.totalIgnoradosDadosInvalidos;
    summary.totalConsultasCriadas += tokenSummary.totalConsultasCriadas;
    summary.totalConsultasAtivas400 += tokenSummary.totalConsultasAtivas400;
    summary.totalErrosAutorizar += tokenSummary.totalErrosAutorizar;
    summary.totalResultadosEncontrados += tokenSummary.totalResultadosEncontrados;
    summary.totalResultadosSemDados += tokenSummary.totalResultadosSemDados;
    summary.totalLinhasAtualizadas += tokenSummary.totalLinhasAtualizadas;
    summary.totalErrosApi += tokenSummary.totalErrosApi;
    summary.totalErrosDb += tokenSummary.totalErrosDb;
  }

  buildTokenFinalLine(tokenSummary, totalTokens) {
    const tokenErrors =
      tokenSummary.totalErrosApi +
      tokenSummary.totalErrosAutorizar +
      tokenSummary.totalErrosDb;
    return `Finalizado Token ${tokenSummary.tokenPosicao}/${totalTokens} / Clientes ${
      tokenSummary.totalClientesSelecionados
    } (status 200=${tokenSummary.totalConsultasCriadas}, status 400=${
      tokenSummary.totalConsultasAtivas400
    }, erros=${tokenErrors}) | 100% | Tempo total ${formatDurationHhMmSs(
      tokenSummary.durationMs
    )}`;
  }

  buildLastRunSnapshot(summary) {
    if (!summary) {
      return null;
    }

    const totalTokens = Number(summary.totalTokensDisponiveis) || 0;
    const totalErros =
      (summary.totalErrosApi || 0) +
      (summary.totalErrosAutorizar || 0) +
      (summary.totalErrosDb || 0);

    const tokens = (summary.tokensExecutados || []).map((tokenSummary) => {
      const tokenErrors =
        tokenSummary.totalErrosApi +
        tokenSummary.totalErrosAutorizar +
        tokenSummary.totalErrosDb;
      return {
        token_posicao: tokenSummary.tokenPosicao,
        token_total: totalTokens,
        token_id: tokenSummary.tokenId,
        empresa: tokenSummary.empresa || null,
        clientes: tokenSummary.totalClientesSelecionados,
        status_200_consultados: tokenSummary.totalConsultasCriadas,
        status_400_aguardando_resposta_v8: tokenSummary.totalConsultasAtivas400,
        erros: tokenErrors,
        percent: 100,
        tempo_total_hhmmss: formatDurationHhMmSs(tokenSummary.durationMs),
        resumo: this.buildTokenFinalLine(tokenSummary, totalTokens),
      };
    });

    return {
      updated_at: new Date().toISOString(),
      last_cycle: {
        ok: !!summary.ok,
        source: summary.source || null,
        started_at: summary.startedAt || null,
        finished_at: summary.finishedAt || null,
        duration_ms: summary.durationMs || 0,
        duration_hhmmss: formatDurationHhMmSs(summary.durationMs || 0),
        tokens_total: totalTokens,
        tokens_processados: summary.totalTokensProcessados || 0,
        clientes_total: summary.totalClientesSelecionados || 0,
        status_200_consultados: summary.totalConsultasCriadas || 0,
        status_400_aguardando_resposta_v8: summary.totalConsultasAtivas400 || 0,
        erros: totalErros,
        message: summary.message || null,
      },
      tokens,
      linhas_resumo: tokens.map((item) => item.resumo),
    };
  }

  getStatusSnapshot() {
    return this.lastRunSnapshot || {
      updated_at: null,
      last_cycle: null,
      tokens: [],
      linhas_resumo: [],
    };
  }

  async processTokenBatch({ pool, token, tokenIndex, totalTokens, clients }) {
    const tokenStartedAt = Date.now();
    const tokenPosicao = tokenIndex + 1;
    const tokenSummary = buildTokenSummary(tokenPosicao, token);
    const safeClients = Array.isArray(clients) ? clients : [];

    info(`Token ${tokenPosicao}/${totalTokens}`);
    if (this.statusTracker) {
      this.statusTracker.incrementWindow();
    }

    try {
      await this.waitForTokenHourlyWindow(token, tokenPosicao, totalTokens);
      tokenSummary.totalClientesSelecionados = safeClients.length;
      info(
        `Token ${tokenPosicao}/${totalTokens} | Clientes separados ${safeClients.length}`
      );

      const totalClientes = safeClients.length;
      const progressStartedAt = Date.now();
      let lastLoggedPercent = -1;
      const logCompactProgress = (current) => {
        const percent = progressPercent(current, totalClientes);
        if (!shouldLogProgress(lastLoggedPercent, percent, current, totalClientes)) {
          return;
        }
        lastLoggedPercent = percent;
        const eta = estimateEtaText(current, totalClientes, progressStartedAt);
        info(
          `Token ${tokenPosicao}/${totalTokens} | Cliente ${current}/${totalClientes} - ${percent}% ${compactProgressText(
            current,
            totalClientes
          )} | ETA ${eta}`
        );
      };
      logCompactProgress(0);

      for (const [index, client] of safeClients.entries()) {
        const posicao = index + 1;

        if (!hasValidClientData(client)) {
          tokenSummary.totalIgnoradosDadosInvalidos += 1;
          logCompactProgress(posicao);
          continue;
        }

        const itemSummary = await this.processClient(
          pool,
          token.access_token,
          client,
          token.empresa
        );
        tokenSummary.totalProcessados += 1;
        tokenSummary.totalConsultasCriadas += itemSummary.consultasCriadas;
        tokenSummary.totalConsultasAtivas400 += itemSummary.consultasAtivas400;
        tokenSummary.totalErrosAutorizar += itemSummary.errosAutorizar;
        tokenSummary.totalResultadosEncontrados += itemSummary.resultadosEncontrados;
        tokenSummary.totalResultadosSemDados += itemSummary.resultadosSemDados;
        tokenSummary.totalLinhasAtualizadas += itemSummary.linhasAtualizadas;
        tokenSummary.totalErrosApi += itemSummary.errosApi;
        tokenSummary.totalErrosDb += itemSummary.errosDb;
        logCompactProgress(posicao);

        if (this.jobConfig.waitBetweenClientsMs > 0) {
          await sleep(this.jobConfig.waitBetweenClientsMs);
        }
      }
    } catch (err) {
      tokenSummary.ok = false;
      tokenSummary.message = err.message;
      tokenSummary.totalErrosDb += 1;
      this.recordDbError("job:token_batch", 500, err.message);
      error(
        `Falha durante processamento do token ${tokenPosicao}/${totalTokens}: ${err.message}`
      );
    }

    tokenSummary.durationMs = Date.now() - tokenStartedAt;
    const tokenErrors =
      tokenSummary.totalErrosApi +
      tokenSummary.totalErrosAutorizar +
      tokenSummary.totalErrosDb;
    info(
      `Finalizado Token ${tokenPosicao}/${totalTokens} / Clientes ${
        tokenSummary.totalClientesSelecionados
      } (status 200=${tokenSummary.totalConsultasCriadas}, status 400=${
        tokenSummary.totalConsultasAtivas400
      }, erros=${tokenErrors}) | 100% | Tempo total ${formatDurationHhMmSs(
        tokenSummary.durationMs
      )}`
    );

    return tokenSummary;
  }

  async run(source = "manual") {
    if (this.isRunning) {
      return {
        ok: false,
        reason: "already_running",
        message: "Ja existe uma execucao em andamento.",
      };
    }

    this.isRunning = true;
    const startedAt = new Date();
    let cycleStarted = false;
    const summary = {
      ok: true,
      source,
      startedAt: startedAt.toISOString(),
      finishedAt: null,
      durationMs: 0,
      totalTokensDisponiveis: 0,
      totalTokensProcessados: 0,
      tokensExecutados: [],
      totalClientesSelecionados: 0,
      totalProcessados: 0,
      totalIgnoradosDadosInvalidos: 0,
      totalConsultasCriadas: 0,
      totalConsultasAtivas400: 0,
      totalErrosAutorizar: 0,
      totalResultadosEncontrados: 0,
      totalResultadosSemDados: 0,
      totalLinhasAtualizadas: 0,
      totalErrosApi: 0,
      totalErrosDb: 0,
    };

    try {
      if (this.statusTracker) {
        this.statusTracker.startCycle(source);
        cycleStarted = true;
      }

      info("Iniciando...");

      const pool = await getPool(this.dbConfig);

      try {
        await ensureDescricaoColumn(pool);
        await ensureTokenUsadoColumn(pool);
      } catch (err) {
        summary.totalErrosDb += 1;
        this.recordDbError("sql:ensure_columns", 500, err.message);
        throw err;
      }

      let latestTokens;
      try {
        latestTokens = await getLatestTokensByEmpresa(pool);
      } catch (err) {
        summary.totalErrosDb += 1;
        this.recordDbError("sql:get_latest_tokens", 500, err.message);
        throw err;
      }

      if (!latestTokens || latestTokens.length === 0) {
        summary.ok = false;
        summary.message = "Nenhum token encontrado na tabela tokens_v8.";
        const finished = this.finishSummary(summary, startedAt);
        this.lastRunSnapshot = this.buildLastRunSnapshot(finished);
        return finished;
      }
      summary.totalTokensDisponiveis = latestTokens.length;
      info(`${latestTokens.length} Tokens encontrados`);

      const totalTokens = latestTokens.length;
      const maxPerToken = Math.min(
        MAX_CONSULTAS_HORA_POR_TOKEN,
        Math.max(
          1,
          Number.parseInt(this.jobConfig.maxClientsPerToken, 10) ||
            MAX_CONSULTAS_HORA_POR_TOKEN
        )
      );
      const totalClientsLimit = totalTokens * maxPerToken;

      let cycleClients;
      try {
        cycleClients = await getPendingClientsBatch(pool, totalClientsLimit);
      } catch (err) {
        summary.totalErrosDb += 1;
        this.recordDbError("sql:get_pending_clients_batch", 500, err.message);
        throw err;
      }

      info(
        `Clientes separados ${cycleClients.length} (sem repeticao) para ${totalTokens} tokens, max ${maxPerToken} por token`
      );

      const clientsByToken = splitClientsAcrossTokens(
        cycleClients,
        totalTokens,
        maxPerToken
      );
      const tokenSummaries = await Promise.all(
        latestTokens.map((token, tokenIndex) =>
          this.processTokenBatch({
            pool,
            token,
            tokenIndex,
            totalTokens,
            clients: clientsByToken[tokenIndex] || [],
          })
        )
      );

      for (const tokenSummary of tokenSummaries) {
        summary.totalTokensProcessados += 1;
        summary.tokensExecutados.push(tokenSummary);
        this.mergeTokenSummary(summary, tokenSummary);
      }

      const failedTokens = tokenSummaries.filter((tokenSummary) => !tokenSummary.ok);
      if (failedTokens.length > 0) {
        summary.ok = false;
        summary.message = `Falha em ${failedTokens.length} token(s): ${failedTokens
          .map((tokenSummary) => tokenSummary.tokenPosicao)
          .join(", ")}`;
      }

      const finished = this.finishSummary(summary, startedAt);
      const totalErros =
        finished.totalErrosApi +
        finished.totalErrosAutorizar +
        finished.totalErrosDb;
      info(
        `Ciclo finalizado: tokens ${finished.totalTokensProcessados}/${finished.totalTokensDisponiveis}, clientes ${finished.totalProcessados}/${finished.totalClientesSelecionados}, status 200=${finished.totalConsultasCriadas}, status 400=${finished.totalConsultasAtivas400}, erros=${totalErros}, tempo total=${formatDurationHhMmSs(
          finished.durationMs
        )}`
      );
      info("Finalizado, proximo fluxo comeca em 1 hora");
      this.lastRunSnapshot = this.buildLastRunSnapshot(finished);
      return finished;
    } catch (err) {
      summary.ok = false;
      summary.message = err.message;
      if (summary.totalErrosDb === 0) {
        summary.totalErrosDb += 1;
        this.recordDbError("job:run", 500, err.message);
      }
      error(`Falha durante a execucao do ciclo: ${err.message}`);
      const finished = this.finishSummary(summary, startedAt);
      info(
        `Ciclo finalizado com erro: ${finished.message || "erro_nao_informado"} | tempo total=${formatDurationHhMmSs(
          finished.durationMs
        )}`
      );
      this.lastRunSnapshot = this.buildLastRunSnapshot(finished);
      return finished;
    } finally {
      if (cycleStarted && this.statusTracker) {
        this.statusTracker.completeCycle();
      }
      this.isRunning = false;
    }
  }

  finishSummary(summary, startedAt) {
    const finishedAt = new Date();
    summary.finishedAt = finishedAt.toISOString();
    summary.durationMs = finishedAt.getTime() - startedAt.getTime();
    return summary;
  }

  finalizeClientResult(result) {
    return result;
  }

  async processClient(pool, accessToken, client, tokenEmpresa) {
    const result = {
      consultasCriadas: 0,
      consultasAtivas400: 0,
      errosAutorizar: 0,
      resultadosEncontrados: 0,
      resultadosSemDados: 0,
      linhasAtualizadas: 0,
      errosApi: 0,
      errosDb: 0,
      api1Status: null,
      api2Status: null,
      api3Status: null,
    };
    const context = {
      clientId: client.id,
      cpf: String(client.cliente_cpf || ""),
      nome: String(client.cliente_nome || ""),
      tokenEmpresa: hasValue(tokenEmpresa) ? String(tokenEmpresa).trim() : null,
    };

    let shouldGetResult = false;
    let consultId = null;

    try {
      const consultResponse = await this.v8Client.createConsult(accessToken, client);
      result.api1Status = consultResponse.status;

      if (consultResponse.status >= 200 && consultResponse.status < 300) {
        result.consultasCriadas += 1;
        shouldGetResult = true;
        consultId = consultResponse.data?.id || null;
      } else if (consultResponse.status === 400) {
        result.consultasAtivas400 += 1;
        shouldGetResult = true;
      } else {
        result.errosApi += 1;
        this.recordApiError(
          "/private-consignment/consult",
          consultResponse.status,
          `API1 retornou status ${consultResponse.status}`
        );
        return this.finalizeClientResult(
          result,
          context,
          false,
          `api1_status_${consultResponse.status}`
        );
      }
    } catch (err) {
      result.errosApi += 1;
      this.recordApiError("/private-consignment/consult", 500, err.message);
      error("Falha de rede ao criar consulta", {
        ...context,
        err: err.message,
      });
      return this.finalizeClientResult(result, context, false, "api1_exception");
    }

    if (this.jobConfig.waitBetweenApisMs > 0) {
      await sleep(this.jobConfig.waitBetweenApisMs);
    }

    if (consultId) {
      try {
        const authorizeResponse = await this.v8Client.authorizeConsult(
          accessToken,
          consultId
        );
        result.api2Status = authorizeResponse.status;

        if (authorizeResponse.status < 200 || authorizeResponse.status >= 300) {
          result.errosAutorizar += 1;
          this.recordApiError(
            "/private-consignment/consult/{id}/authorize",
            authorizeResponse.status,
            `API2 retornou status ${authorizeResponse.status}`
          );
        }
      } catch (err) {
        result.errosAutorizar += 1;
        this.recordApiError(
          "/private-consignment/consult/{id}/authorize",
          500,
          err.message
        );
        error("Falha de rede ao autorizar consulta", {
          ...context,
          consultId,
          err: err.message,
        });
      }
    }

    if (!shouldGetResult) {
      return this.finalizeClientResult(result, context, false, "sem_consulta");
    }

    if (this.jobConfig.waitBetweenApisMs > 0) {
      await sleep(this.jobConfig.waitBetweenApisMs);
    }

    try {
      const consultResultResponse = await this.v8Client.getConsultResult(
        accessToken,
        client.cliente_cpf
      );
      result.api3Status = consultResultResponse.status;

      if (
        consultResultResponse.status < 200 ||
        consultResultResponse.status >= 300
      ) {
        result.errosApi += 1;
        this.recordApiError(
          "/private-consignment/consult",
          consultResultResponse.status,
          `API3 retornou status ${consultResultResponse.status}`
        );
        return this.finalizeClientResult(
          result,
          context,
          false,
          `api3_status_${consultResultResponse.status}`
        );
      }

      const apiData = consultResultResponse.data?.data;
      if (!Array.isArray(apiData) || apiData.length === 0) {
        result.resultadosSemDados += 1;
        return this.finalizeClientResult(result, context, false, "api3_sem_dados");
      }

      const payload = this.extractPayloadForUpdate(apiData, tokenEmpresa);
      if (!payload) {
        result.resultadosSemDados += 1;
        return this.finalizeClientResult(result, context, false, "payload_invalido");
      }

      let rowsAffected;
      try {
        rowsAffected = await updateClientByCpf(pool, payload);
      } catch (err) {
        result.errosDb += 1;
        this.recordDbError("sql:update_client_by_cpf", 500, err.message);
        error("Falha no update SQL", {
          ...context,
          err: err.message,
        });
        return this.finalizeClientResult(result, context, false, "db_update_exception");
      }

      result.resultadosEncontrados += 1;
      result.linhasAtualizadas += rowsAffected;

      if (rowsAffected > 0) {
        return this.finalizeClientResult(result, context, true, "ok", {
          rowsAffected,
        });
      }

      return this.finalizeClientResult(result, context, false, "merge_sem_linhas", {
        rowsAffected,
      });
    } catch (err) {
      result.errosApi += 1;
      this.recordApiError("/private-consignment/consult", 500, err.message);
      error("Falha de rede ao buscar resultado da consulta", {
        ...context,
        err: err.message,
      });
      return this.finalizeClientResult(result, context, false, "api3_exception");
    }
  }

  extractPayloadForUpdate(apiData, tokenEmpresa) {
    const first = apiData?.[0] || {};
    const second = apiData?.[1] || {};
    const documentNumber = first.documentNumber ?? second.documentNumber;
    const availableMarginValue =
      first.availableMarginValue ?? second.availableMarginValue;
    const status = first.status ?? second.status;
    const description = first.description ?? second.description;

    if (
      documentNumber === undefined &&
      availableMarginValue === undefined &&
      status === undefined &&
      description === undefined
    ) {
      return null;
    }

    const cpf11 = normalizeCpf(documentNumber);
    if (!cpf11) {
      return null;
    }

    return {
      cpf11,
      valorLiberado: parseMarginValue(availableMarginValue),
      statusConsulta: mapStatus(status),
      descricao: cleanDescription(description),
      tokenUsado: hasValue(tokenEmpresa) ? String(tokenEmpresa).trim() : null,
    };
  }
}

module.exports = {
  ConsignmentJobService,
};
