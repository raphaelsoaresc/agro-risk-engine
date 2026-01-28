-- ==========================================
-- 1. DADOS DE MERCADO & MACRO (Inputs)
-- ==========================================
CREATE TABLE market_prices (
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    close FLOAT,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    volume FLOAT,
    source VARCHAR(50), 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (ticker, date)
);

CREATE TABLE macro_indicators (
    date DATE PRIMARY KEY,
    gold_oil_correlation FLOAT,
    currency_stress_index FLOAT,
    risk_level VARCHAR(20), -- 'NORMAL', 'ALERTA', 'CRITICO'
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE geopolitical_alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    category VARCHAR(50), 
    headline TEXT,
    risk_level VARCHAR(20), 
    is_active BOOLEAN DEFAULT TRUE,
    source_url TEXT
);

-- ==========================================
-- 2. DADOS DE CLIENTES & PORTFÓLIO
-- ==========================================
CREATE TABLE credit_portfolio (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_name VARCHAR(255),
    state_code VARCHAR(2),
    latitude FLOAT,
    longitude FLOAT,
    culture VARCHAR(50),
    loan_amount FLOAT,
    area_hectares FLOAT,
    estimated_yield_kg_ha FLOAT,
    dist_to_port FLOAT,
    credit_score_serasa INT,
    debt_to_income_ratio FLOAT,
    simulation_tag VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==========================================
-- 3. MOTOR DE RISCO (Outputs & Cache)
-- ==========================================
CREATE TABLE risk_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id UUID REFERENCES credit_portfolio(id),
    pd_score FLOAT, 
    risk_level VARCHAR(20),
    details JSONB, -- Justificativa completa da IA
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE climate_historical_cache (
    coordinate_hash VARCHAR(64) PRIMARY KEY, -- MD5(lat_lon_dates)
    latitude FLOAT,
    longitude FLOAT,
    data_json JSONB, -- Dados brutos para não fazer parse complexo
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==========================================
-- 4. VALIDAÇÃO & BACKTEST (Adicionado agora)
-- ==========================================
CREATE TABLE official_crop_stats (
    state_code VARCHAR(2),
    crop_year VARCHAR(20), -- ex: '2023/2024'
    commodity VARCHAR(50),
    yield_kg_ha FLOAT,
    source VARCHAR(50),
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (state_code, crop_year, commodity)
);

CREATE TABLE backtest_simulations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    simulation_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    status VARCHAR(20), -- 'RUNNING', 'COMPLETED'
    total_expected_loss FLOAT,
    avg_log_loss FLOAT,
    max_var_95 FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE backtest_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    simulation_id UUID REFERENCES backtest_simulations(id),
    contract_id UUID REFERENCES credit_portfolio(id),
    sim_date DATE,
    pd_score FLOAT,
    expected_loss FLOAT,
    risk_justification TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==========================================
-- 5. SEGURANÇA (RLS Básico)
-- ==========================================
-- Habilita RLS em todas as tabelas críticas
ALTER TABLE market_prices ENABLE ROW LEVEL SECURITY;
ALTER TABLE credit_portfolio ENABLE ROW LEVEL SECURITY;
ALTER TABLE risk_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE backtest_results ENABLE ROW LEVEL SECURITY;

-- Cria política de leitura pública APENAS para preços (útil para frontend)
CREATE POLICY "Public Read Access" ON market_prices FOR SELECT USING (true);
