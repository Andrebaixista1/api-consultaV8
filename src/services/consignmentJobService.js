const { getPool } = require("../db");
const { info, error } = require("../logger");
const {
  ensureDescricaoColumn,
  getPendingClients,
  updateClientByCpf,
} = require("../repositories/clientRepository");
const {
  getLatestTokensByEmpresa,
} = require("../repositories/tokenRepository");

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

class ConsignmentJobService {
  constructor(options) {
    this.dbConfig = options.dbConfig;
    this.jobConfig = options.jobConfig;
    this.v8Client = options.v8Client;
    this.statusTracker = options.statusTracker || null;
    this.isRunning = false;
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
      } catch (err) {
        summary.totalErrosDb += 1;
        this.recordDbError("sql:ensure_descricao", 500, err.message);
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
        return this.finishSummary(summary, startedAt);
      }
      summary.totalTokensDisponiveis = latestTokens.length;
      info(`${latestTokens.length} Tokens encontrados`);

      const totalTokens = latestTokens.length;

      for (const [tokenIndex, token] of latestTokens.entries()) {
        const tokenStartedAt = Date.now();
        const tokenPosicao = tokenIndex + 1;
        const tokenSummary = {
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
        };
        info(`Token ${tokenPosicao}/${totalTokens}`);
        if (this.statusTracker) {
          this.statusTracker.incrementWindow();
        }

        let clients;
        try {
          clients = await getPendingClients(pool);
        } catch (err) {
          summary.totalErrosDb += 1;
          tokenSummary.totalErrosDb += 1;
          this.recordDbError("sql:get_pending_clients", 500, err.message);
          throw err;
        }

        tokenSummary.totalClientesSelecionados = clients.length;
        summary.totalClientesSelecionados += clients.length;
        info(`Clientes separados ${clients.length}`);

        const totalClientes = clients.length;
        let lastLoggedPercent = -1;
        const logCompactProgress = (current) => {
          const percent = progressPercent(current, totalClientes);
          if (!shouldLogProgress(lastLoggedPercent, percent, current, totalClientes)) {
            return;
          }
          lastLoggedPercent = percent;
          info(
            `Token ${tokenPosicao}/${totalTokens} | Cliente ${current}/${totalClientes} - ${percent}% ${compactProgressText(
              current,
              totalClientes
            )}`
          );
        };
        logCompactProgress(0);

        for (const [index, client] of clients.entries()) {
          const posicao = index + 1;

          if (!hasValidClientData(client)) {
            summary.totalIgnoradosDadosInvalidos += 1;
            tokenSummary.totalIgnoradosDadosInvalidos += 1;
            logCompactProgress(posicao);
            continue;
          }

          const itemSummary = await this.processClient(pool, token.access_token, client);
          summary.totalProcessados += 1;
          summary.totalConsultasCriadas += itemSummary.consultasCriadas;
          summary.totalConsultasAtivas400 += itemSummary.consultasAtivas400;
          summary.totalErrosAutorizar += itemSummary.errosAutorizar;
          summary.totalResultadosEncontrados += itemSummary.resultadosEncontrados;
          summary.totalResultadosSemDados += itemSummary.resultadosSemDados;
          summary.totalLinhasAtualizadas += itemSummary.linhasAtualizadas;
          summary.totalErrosApi += itemSummary.errosApi;
          summary.totalErrosDb += itemSummary.errosDb;

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

        summary.totalTokensProcessados += 1;
        summary.tokensExecutados.push(tokenSummary);
        const tokenDurationMs = Date.now() - tokenStartedAt;
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
            tokenDurationMs
          )}`
        );
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

  async processClient(pool, accessToken, client) {
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

      const payload = this.extractPayloadForUpdate(apiData);
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

  extractPayloadForUpdate(apiData) {
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
    };
  }
}

module.exports = {
  ConsignmentJobService,
};
