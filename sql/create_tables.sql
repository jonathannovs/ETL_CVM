CREATE SCHEMA IF NOT EXISTS cvm;

CREATE TABLE IF NOT EXISTS cvm.fundos (
    cnpj_fundo VARCHAR(16),
    qtd_cotistas INT,
    valor_resgates NUMERIC(18, 6),
    valor_aplicacoes NUMERIC(18, 6),
    cota NUMERIC(18, 6),
    pl_fundo NUMERIC(30, 6),
    data_referencia DATE,
    ano INT,
    dt_ingest DATE
);

