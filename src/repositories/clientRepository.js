const { sql } = require("../db");

async function ensureDescricaoColumn(pool) {
  const query = `
USE apis_v8;

IF COL_LENGTH('dbo.clientes_clt', 'descricao') IS NULL
BEGIN
  ALTER TABLE dbo.clientes_clt
  ADD descricao NVARCHAR(4000) NULL;
END;
`;

  await pool.request().query(query);
}

async function ensureTokenUsadoColumn(pool) {
  const query = `
USE apis_v8;

IF COL_LENGTH('dbo.clientes_clt', 'token_usado') IS NULL
BEGIN
  ALTER TABLE dbo.clientes_clt
  ADD token_usado NVARCHAR(255) NULL;
END;
`;

  await pool.request().query(query);
}

async function getPendingClients(pool) {
  const query = `
USE apis_v8;

SELECT TOP(250) *
FROM dbo.clientes_clt
WHERE status_consulta_v8 IN ('Aguardando', 'Aguardando Consulta', 'Consentimento Aprovado')
ORDER BY status_consulta_v8 DESC, NEWID();
`;

  const result = await pool.request().query(query);
  return result.recordset || [];
}

async function getPendingClientsBatch(pool, limit) {
  const safeLimit = Math.max(0, Number.parseInt(limit, 10) || 0);
  if (safeLimit <= 0) {
    return [];
  }

  const query = `
USE apis_v8;

;WITH pending AS (
  SELECT
    *,
    RIGHT(
      REPLICATE('0', 11) +
      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(cliente_cpf, ''))), '.', ''), '-', ''), '/', ''), ' ', ''), CHAR(9), ''),
      11
    ) AS cpf11,
    ROW_NUMBER() OVER (
      PARTITION BY RIGHT(
        REPLICATE('0', 11) +
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(cliente_cpf, ''))), '.', ''), '-', ''), '/', ''), ' ', ''), CHAR(9), ''),
        11
      )
      ORDER BY
        CASE status_consulta_v8
          WHEN 'Consentimento Aprovado' THEN 3
          WHEN 'Aguardando Consulta' THEN 2
          WHEN 'Aguardando' THEN 1
          ELSE 0
        END DESC,
        NEWID()
    ) AS cpf_rownum
  FROM dbo.clientes_clt
  WHERE status_consulta_v8 IN ('Aguardando', 'Aguardando Consulta', 'Consentimento Aprovado')
)
SELECT TOP (@limit) *
FROM pending
WHERE cpf_rownum = 1
ORDER BY
  CASE status_consulta_v8
    WHEN 'Consentimento Aprovado' THEN 3
    WHEN 'Aguardando Consulta' THEN 2
    WHEN 'Aguardando' THEN 1
    ELSE 0
  END DESC,
  NEWID();
`;

  const request = pool.request();
  request.input("limit", sql.Int, safeLimit);
  const result = await request.query(query);
  return result.recordset || [];
}

async function updateClientByCpf(pool, payload) {
  const query = `
USE apis_v8;

UPDATE dbo.clientes_clt
SET
  valor_liberado = COALESCE(@valor_liberado, valor_liberado),
  created_at = SYSDATETIME(),
  status_consulta_v8 = COALESCE(@status_consulta_v8, status_consulta_v8),
  descricao = @descricao,
  token_usado = COALESCE(@token_usado, token_usado)
WHERE
  RIGHT(
    REPLICATE('0', 11) +
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(cliente_cpf, ''))), '.', ''), '-', ''), '/', ''), ' ', ''), CHAR(9), ''),
    11
  ) = @cpf11;

SELECT @@ROWCOUNT AS rows_affected;
`;

  const request = pool.request();
  request.input("cpf11", sql.VarChar(11), payload.cpf11);
  request.input("valor_liberado", sql.Decimal(18, 2), payload.valorLiberado);
  request.input("status_consulta_v8", sql.NVarChar(60), payload.statusConsulta);
  request.input("descricao", sql.NVarChar(4000), payload.descricao);
  request.input("token_usado", sql.NVarChar(255), payload.tokenUsado);

  const result = await request.query(query);
  return result.recordset?.[0]?.rows_affected || 0;
}

module.exports = {
  ensureDescricaoColumn,
  ensureTokenUsadoColumn,
  getPendingClients,
  getPendingClientsBatch,
  updateClientByCpf,
};
