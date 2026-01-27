Esta é a **Documentação Técnica de Referência (Technical Reference Guide)** do sistema **Agro Risk Monitor - High Ticket (v2.6.0)**. Este documento foi estruturado para servir como guia definitivo para transferência de propriedade, auditoria e manutenção futura.

---

# 🛰️ Agro Risk Monitor - Technical Reference Guide

## 1. Visão Geral da Arquitetura
O sistema é uma plataforma de inteligência de risco agrícola baseada em dados, projetada para monitorar variáveis climáticas, logísticas e de mercado. A arquitetura segue o padrão **Modular Monolith** com separação clara entre ingestão de dados, processamento de risco e interface de visualização.

### Fluxo de Dados Macro:
1.  **Ingestão (Ingestion Layer):** Scripts de coleta (`worker_market.py`, `flow/`) buscam dados em tempo real de APIs externas (Yahoo Finance, Open-Meteo, Google News).
2.  **Persistência (Storage Layer):** Os dados brutos e processados são armazenados no **Supabase** (PostgreSQL).
3.  **Processamento (Intelligence Engine):** O `RiskPipeline` orquestra a análise. Ele utiliza o **Strategy Pattern** para aplicar regras de risco específicas por região (MT, PR) e o **Facade Pattern** para indicadores técnicos e financeiros.
4.  **Saída (Output Layer):**
    *   **Dashboard:** Interface Streamlit para visualização em tempo real.
    *   **Relatórios:** Geração de PDFs via `FPDF` e envio de e-mails via `MailerSend`.

---

## 2. Dicionário de Scripts e Módulos

### 📂 Core (O Coração do Sistema)
*   **`core/pipeline.py`**: Orquestrador principal. Coordena a carga de dados, execução dos motores de risco e disparo de notificações.
*   **`core/engine.py`**: Motor de risco genérico. Contém a lógica de sanitização de dados e cálculos de volatilidade/tendência.
*   **`core/db.py`**: Gerenciador de conexão com o Supabase. Implementa cache de conexão para evitar exaustão de recursos.
*   **`core/climate_risk.py`**: Interface com APIs de clima. Possui lógica de *fallback* sintético caso as APIs falhem.
*   **`core/scout.py`**: Módulo de OSINT (Open Source Intelligence). Usa IA (Hugging Face) para classificar notícias em "Crítico", "Oportunidade" ou "Neutro".
*   **`core/report_generator.py`**: Responsável pela construção visual do PDF, incluindo geração de gráficos via Matplotlib.

### 📂 Interface (Frontend)
*   **`dashboard.py`**: Ponto de entrada da interface Streamlit.
*   **`interface/auth.py`**: Gerenciamento de sessão e integração com Supabase Auth.
*   **`interface/pages.py`**: Lógica de renderização das abas (Visão Geral, Mercado, Clima).
*   **`interface/components.py`**: Componentes visuais reutilizáveis (Cards de KPI, Gráficos Plotly).

### 📂 Flow & Workers (Automação)
*   **`worker_market.py`**: Script independente para coleta de preços de commodities e câmbio.
*   **`flow/origination.py` / `export.py`**: Lógicas específicas para monitorar o interior (fazendas) e os portos.

---

## 3. Detalhamento de Código

### Classes Principais

| Classe | Responsabilidade | Herança/Dependência |
| :--- | :--- | :--- |
| `RiskPipeline` | Coordena o ciclo de vida da análise de risco. | `DatabaseManager`, `RiskEngine` |
| `BaseRiskStrategy` | Classe abstrata que define a interface para riscos regionais. | `ABC` |
| `MatoGrossoStrategy` | Implementa regras específicas (ex: Regra do Diesel/Distância). | `BaseRiskStrategy` |
| `DatabaseManager` | Singleton para operações de CRUD no Supabase. | `supabase-py` |
| `NewsScout` | Coleta e analisa notícias via RSS e NLP. | `httpx`, `HuggingFace API` |

### Funções e Métodos Críticos

#### `RiskEngine.calculate_full_analysis`
*   **O que faz:** Realiza o cruzamento de dados de mercado, clima e logística para um local específico.
*   **Parâmetros:** `df_market` (DataFrame), `loc_name` (String), `df_climate` (DataFrame).
*   **Retorno:** Um dicionário de scores (0-100) e um dicionário de métricas detalhadas.

#### `FundamentalIndicators.calculate_washout_probability`
*   **O que faz:** Calcula a probabilidade de quebra de contrato (default) baseada no estresse climático e variação de preço.
*   **Lógica:** Se o preço sobe muito (incentivo ao default) e o clima é crítico (falta de produto), o score atinge o nível máximo.

#### `ClimateIntelligence.run_full_scan`
*   **O que faz:** Varredura assíncrona de múltiplos pontos geográficos.
*   **Diferencial:** Implementa um **Semáforo de Concorrência** para não ser bloqueado pelas APIs de clima.

---

## 4. Variáveis Críticas e Configurações

### Arquivo `configs/settings.yaml`
Este é o cérebro configurável do projeto.
*   **`tickers`**: Lista de ativos monitorados (ZS=F para Soja, USDBRL=X para Dólar).
*   **`risk_thresholds`**: Gatilhos numéricos (ex: `rsi_overbought: 70`).
*   **`locations`**: Lista de dicionários contendo lat/lon e `dist_to_port` (usado no cálculo de frete).

### Variáveis de Ambiente (`.env`)
*   `SUPABASE_URL` / `SUPABASE_SERVICE_ROLE_KEY`: Acesso ao banco de dados.
*   `HUGGINGFACE_API_KEY`: Token para o modelo de classificação de notícias.
*   `MAILERSEND_API_KEY`: Token para envio de e-mails transacionais.

---

## 5. Mapeamento de Relações
1.  **`main.py`** chama **`RiskPipeline`**.
2.  **`RiskPipeline`** consome **`MarketLoader`** para obter dados de preços.
3.  **`RiskPipeline`** instancia a estratégia correta via **`RegionalEngineFactory`**.
4.  A estratégia (ex: **`ParanaStrategy`**) usa **`TechnicalIndicators`** para validar a volatilidade.
5.  O resultado é enviado para o **`RiskPersister`**, que grava no Supabase.
6.  O **`RiskNotifier`** verifica se o score global ultrapassou o limite e aciona o **`MailerSend`**.

---

## 6. Guia de Manutenção

### Como adicionar uma nova região de monitoramento?
1.  Crie uma nova classe em `core/strategies/` herdando de `BaseRiskStrategy`.
2.  Implemente os métodos de cálculo de risco logístico, climático e de mercado.
3.  Registre a nova região no `RegionalEngineFactory` dentro de `core/factory.py`.

### Como alterar a sensibilidade do risco de mercado?
*   Vá até `core/engine.py` no método `_calculate_calibrated_market_score`.
*   Ajuste os pesos de `vol_score` e `price_risk`.

### Como modificar o layout do e-mail?
*   Altere os templates HTML em `core/reporting_template.py`.

---

## 7. Dependências Externas e APIs
*   **Yahoo Finance (yfinance):** Dados históricos e tempo real de commodities.
*   **Open-Meteo API:** Previsão de precipitação e temperatura (Gratuito/Open Source).
*   **WeatherAPI:** Fallback secundário para dados climáticos.
*   **Supabase:** Backend-as-a-Service (PostgreSQL, Auth, Realtime).
*   **Hugging Face (BART Model):** Classificação de texto (Zero-shot classification) para notícias.
*   **MailerSend:** Infraestrutura de envio de e-mails.

---

**Nota de Segurança:** Este sistema utiliza chaves de API sensíveis. Certifique-se de que o arquivo `.env` nunca seja versionado em repositórios públicos. A integridade dos dados depende da execução regular do `worker_market.py`.