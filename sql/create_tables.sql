CREATE SCHEMA IF NOT EXISTS cvm;

CREATE TABLE IF NOT EXISTS cvm.fundos (
    id BIGSERIAL PRIMARY KEY,
    cnpj_fundo VARCHAR(14),
    qtd_cotistas INT,
    valor_resgates NUMERIC(30, 6),
    valor_aplicacoes NUMERIC(30, 6),
    cota NUMERIC(18, 6),
    valor_carteira NUMERIC(30, 6),
    pl_fundo NUMERIC(30, 6),
    data_referencia DATE,
    ano INT,
    dt_ingest DATE,
    UNIQUE (cnpj_fundo, data_referencia)
);

