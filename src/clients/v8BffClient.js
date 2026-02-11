const axios = require("axios");

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

class V8BffClient {
  constructor(options) {
    this.provider = options.provider;
    this.defaultSignerPhone = options.signerPhone;
    this.http = axios.create({
      baseURL: options.baseUrl,
      timeout: options.httpTimeoutMs,
      validateStatus: () => true,
    });
  }

  buildHeaders(accessToken) {
    return {
      Authorization: `Bearer ${accessToken}`,
    };
  }

  async createConsult(accessToken, client) {
    const body = {
      borrowerDocumentNumber: String(client.cliente_cpf || ""),
      gender: String(client.cliente_sexo || ""),
      birthDate: toBirthDate(client.nascimento),
      signerName: String(client.cliente_nome || ""),
      signerEmail: String(client.email || ""),
      signerPhone: parseSignerPhone(client.telefone, this.defaultSignerPhone),
      provider: this.provider,
    };

    return this.http.post("/private-consignment/consult", body, {
      headers: this.buildHeaders(accessToken),
    });
  }

  async authorizeConsult(accessToken, consultId) {
    return this.http.post(
      `/private-consignment/consult/${consultId}/authorize`,
      {},
      {
        headers: this.buildHeaders(accessToken),
      }
    );
  }

  async getConsultResult(accessToken, cpf) {
    const { startDate, endDate } = utcDayRange();

    return this.http.get("/private-consignment/consult", {
      headers: this.buildHeaders(accessToken),
      params: {
        startDate,
        endDate,
        limit: 50,
        page: 1,
        search: String(cpf || ""),
        provider: this.provider,
      },
    });
  }
}

module.exports = {
  V8BffClient,
};
