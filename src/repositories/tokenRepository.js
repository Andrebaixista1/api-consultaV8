async function getLatestTokensByEmpresa(pool) {
  const query = `
USE apis_v8;

;WITH x AS (
  SELECT
      t.*,
      ROW_NUMBER() OVER (PARTITION BY t.empresa ORDER BY t.created_at DESC, t.id DESC) AS rn
  FROM dbo.tokens_v8 t
)
SELECT
    id,
    access_token,
    expires_in,
    created_at,
    empresa
FROM x
WHERE rn = 1
ORDER BY id ASC;
`;

  const result = await pool.request().query(query);
  return result.recordset || [];
}

module.exports = {
  getLatestTokensByEmpresa,
};
