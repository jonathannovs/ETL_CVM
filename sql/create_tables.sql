CREATE SCHEMA IF NOT EXISTS cvm;

CREATE TABLE IF NOT EXISTS cvm.fundos (
    id_fund_date VARCHAR(30) PRIMARY KEY,
    cnpj_fundo VARCHAR(16),
    nome_fundo VARCHAR(256),
    qtd_cotistas INT,
    valor_resgates NUMERIC(18, 6),
    valor_aplicacoes NUMERIC(18, 6),
    cota NUMERIC(18, 6),
    pl_fundo NUMERIC(30, 6),
    data_referencia DATE,
    ano INT,
    dt_ingest DATE
);
    
CREATE TABLE IF NOT EXISTS cvm.metricas (
    id_fund_date VARCHAR(30) PRIMARY KEY,
    cnpj_fundo VARCHAR(16),
    nome_fundo VARCHAR(256),
    valor_resgates NUMERIC(18, 6),
    valor_aplicacoes NUMERIC(18, 6),
    cota NUMERIC(18, 6),
    pl_fundo NUMERIC(30, 6),
    pct_rentabilidade_diaria NUMERIC(30, 6),
    net  NUMERIC(30, 6),
    pnl NUMERIC(30, 6),
    data_referencia DATE,
    ano INT,
    dt_ingest DATE
);

