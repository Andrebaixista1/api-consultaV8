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

async function updateClientByCpf(pool, payload) {
  const query = `
USE apis_v8;

UPDATE dbo.clientes_clt
SET
  valor_liberado = COALESCE(@valor_liberado, valor_liberado),
  created_at = SYSDATETIME(),
  status_consulta_v8 = COALESCE(@status_consulta_v8, status_consulta_v8),
  descricao = @descricao
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

  const result = await request.query(query);
  return result.recordset?.[0]?.rows_affected || 0;
}

module.exports = {
  ensureDescricaoColumn,
  getPendingClients,
  updateClientByCpf,
};
