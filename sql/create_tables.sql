CREATE SCHEMA IF NOT EXISTS cvm;

CREATE TABLE IF NOT EXISTS cvm.fundos (
 cnpj_fundo VARCHAR(14),
 qtd_cotistas INT,
 valor_resgates FLOAT,
 valor_aplicacoes FLOAT,
 cota FLOAT,
 valor_carteira FLOAT,
 pl_fundo FLOAT,
 data_referencia DATE,
 ano INT,
 dt_ingest DATE
);

