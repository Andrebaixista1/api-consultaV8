const sql = require("mssql");

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

async function getPool(dbConfig) {
  if (!poolPromise) {
    const pool = new sql.ConnectionPool(createSqlConfig(dbConfig));
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

module.exports = {
  sql,
  getPool,
  closePool,
};
