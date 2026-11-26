-- Execute no SQL Editor. Para outro catálogo, substitua case_nyc_taxi.
-- Esta auditoria independe do update SDP, que pode nem iniciar após uma falha HTTP.

-- 1. Resumo de execução por categoria: previstos, publicados, reaproveitados e erros.
SELECT execucao_id, dataset, categoria, periodos_solicitados, status,
       arquivos_previstos, arquivos_ok, arquivos_existentes, arquivos_erro,
       duracao_ms / 1000.0 AS duracao_s, ingested_at
FROM case_nyc_taxi.governanca.log_ingestao
WHERE evento = 'fim_lote'
ORDER BY ingested_at DESC;

-- 2. Resultado mais recente por partição (uma falha histórica não esconde recuperação).
WITH ultima AS (
    SELECT *, row_number() OVER (
        PARTITION BY dataset, categoria, ano, mes ORDER BY ingested_at DESC
    ) AS ordem
    FROM case_nyc_taxi.governanca.log_ingestao
    WHERE evento = 'fim_arquivo'
)
SELECT categoria, mes_ano, status, operacao, tentativa, http_status, erro_tipo,
       mensagem, destino, tamanho_bytes, linhas, checksum, execucao_id, ingested_at
FROM ultima WHERE ordem = 1
ORDER BY ano DESC, mes DESC, categoria;

-- 3. Erros, tentativas e volume efetivamente transferido nas últimas 24h.
SELECT categoria, mes_ano, http_status, erro_tipo,
       count(*) AS tentativas,
       count_if(vai_repetir) AS retries,
       count_if(status = 'erro') AS falhas_finais,
       sum(bytes_recebidos) AS bytes_transferidos,
       sum(espera_s) FILTER (WHERE vai_repetir) AS espera_retry_s,
       avg(duracao_ms) / 1000.0 AS duracao_media_s
FROM case_nyc_taxi.governanca.log_ingestao
WHERE evento = 'tentativa' AND ingested_at >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY categoria, mes_ano, http_status, erro_tipo;

-- 4. Cobertura do plano solicitado; revela inclusive processo morto antes de tentar um mês.
WITH planos AS (
    SELECT execucao_id, dataset, categoria, explode(split(periodos_solicitados, ',')) AS mes_ano
    FROM case_nyc_taxi.governanca.log_ingestao WHERE evento = 'inicio_lote'
), finais AS (
    SELECT execucao_id, dataset, mes_ano, status
    FROM case_nyc_taxi.governanca.log_ingestao WHERE evento = 'fim_arquivo'
)
SELECT p.*, coalesce(f.status, 'sem_resultado') AS status
FROM planos p LEFT JOIN finais f USING (execucao_id, dataset, mes_ano)
WHERE coalesce(f.status, 'sem_resultado') NOT IN ('ok', 'ja_existia');

-- 5. Candidatos a alerta: execução sem fim e sem eventos há mais de uma hora.
-- Ajuste o limiar à política HTTP e ao tempo real do seu armazenamento.
SELECT execucao_id, dataset, max(ingested_at) AS ultimo_evento
FROM case_nyc_taxi.governanca.log_ingestao
WHERE execucao_id IS NOT NULL
GROUP BY execucao_id, dataset
HAVING count_if(evento = 'fim_lote') = 0
   AND max(ingested_at) < current_timestamp() - INTERVAL 1 HOUR;
