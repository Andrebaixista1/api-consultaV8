# v8-consignment-api

API Node.js para reproduzir o fluxo do n8n:

1. Busca os tokens mais recentes por empresa em `tokens_v8` e monta uma fila (ordem por `id`).
2. Para cada token da fila, busca `TOP(250)` clientes com status pendente em `clientes_clt`.
3. Para cada cliente valido do token atual:
   - `POST /private-consignment/consult`
   - usa `telefone` do proprio cliente para montar `signerPhone`
   - espera entre APIs (`WAIT_BETWEEN_APIS_MS`, default 3000 ms)
   - `POST /private-consignment/consult/{id}/authorize`
   - `GET /private-consignment/consult` filtrando por CPF
4. Atualiza `clientes_clt` com `valor_liberado`, `status_consulta_v8`, `descricao` e `created_at`.
5. Ao terminar todos os tokens, espera o proximo ciclo do scheduler (default: 1 hora) e reinicia do token 1.

Os logs mostram progresso visual:
- fluxo compacto por token/cliente (menos ruido)
- exemplo:
  - `Iniciando...`
  - `7 Tokens encontrados`
  - `Token 1/7`
  - `Clientes separados 250`
  - `Token 1/7 | Cliente 0/250 - 0% [----------------------] 0/250 (0%) | ETA --:--:--`
  - `...`
  - `Finalizado Token 1/7 / Clientes 250 (status 200=120, status 400=90, erros=40) | 100% | Tempo total 00:30:00`
  - `Finalizado, proximo fluxo comeca em 1 hora`

## Requisitos

- Node.js 18+

## Configuracao

1. Copie `.env.example` para `.env`
2. Preencha principalmente `DB_PASSWORD`

## Executar

```bash
npm install
npm start
```

## Endpoint

- `GET /health`
- `GET /api/status`
  - retorna status do ciclo atual/ultimo ciclo + erros de API/DB no formato para Postman
  - exemplo de resposta:

```json
{
  "current_cycle": null,
  "last_cycle": {
    "started_at": "2026-02-11 17:36:57",
    "completed_at": "2026-02-11 17:36:57",
    "period": {
      "start": "2026-02-11",
      "end": "2026-02-11"
    },
    "duration_ms": 134,
    "duration_hhmmssmmmm": "00:00:00:0134",
    "api_errors": 0,
    "db_errors": 0,
    "total_windows": 1,
    "had_api_errors": false,
    "had_db_errors": false
  },
  "api_errors": {
    "total_count": 6,
    "last_cycle_count": 0,
    "last": null,
    "had_errors_in_current_cycle": false
  },
  "db_errors": {
    "total_count": 2034,
    "last_cycle_count": 0,
    "last": null,
    "had_errors_in_current_cycle": false
  },
  "last_error": null,
  "server_time": "2026-02-11 17:36:58",
  "status_server": {
    "host": "0.0.0.0",
    "port": "3066"
  }
}
```
- `POST /api/jobs/run`
  - dispara manualmente o ciclo completo de tokens
  - exemplo:

```bash
curl -X POST "http://localhost:3000/api/jobs/run"
```

## Variaveis principais

- `HOST`: host HTTP da API (`0.0.0.0` por padrao).
- `WAIT_BETWEEN_APIS_MS`: espera entre chamadas de API (API1->API2 e API2/pulo->API3).
- `WAIT_BETWEEN_CLIENTS_MS`: pausa entre clientes.
- `SCHEDULER_ENABLED`: liga/desliga execucao automatica.
- `SCHEDULER_CRON`: cron do scheduler (`0 * * * *` = a cada 1 hora).
- `JOB_RUN_ON_STARTUP`: se `true`, dispara 1 execucao imediatamente ao iniciar a API.
