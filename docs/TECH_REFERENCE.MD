# 📘 Agro Risk Engine DaaS - Technical Reference Guide

**Versão do Documento:** 1.0.0
**Status:** Production Core / MVP
**Auditoria Técnica:** Raphael Soares
**Stack Principal:** Python 3.10+, FastAPI, Supabase (PostgreSQL), Pandas, AsyncIO.
**Infraestrutura:** Nix (Ambiente Hermético) + uv (Gerenciamento de Pacotes).

> **Nota:** Este documento detalha a arquitetura, decisões de design e fluxo de dados do Motor de Risco. Destina-se a desenvolvedores e arquitetos que necessitam compreender a lógica interna e os padrões de projeto adotados.

---

## 1. Camada de Configuração e Ambiente

### 📄 `settings.yaml` & `core/env.py`
**Propósito:** Centralização de parâmetros de negócio e segredos de infraestrutura. Separa a lógica da parametrização.

**Stack Específica:**
*   **YAML:** Padrão de indústria para configuração legível.
*   **Python (`pyyaml`):** Carregamento dinâmico com suporte a overrides de ambiente.

**Decisões de Engenharia:**
*   **Desacoplamento Geográfico:** Coordenadas (`lat`, `lon`) e clusters são definidos no YAML, evitando *hardcoding*. Isso permite adicionar novas regiões de monitoramento sem necessidade de *redeploy* da aplicação, apenas alteração de configuração.
*   **Singleton Implícito:** O módulo `core/env.py` carrega as configurações uma única vez na inicialização, garantindo consistência durante o ciclo de vida da aplicação.

**Mapeamento de Dependências:**
*   **Consumidores:** `core/pipeline.py`, `core/market_router.py`, `core/strategies/base.py`.
*   **Fluxo:** Dependência crítica (Blocker). O sistema não inicializa sem este módulo.

---

## 2. Pontos de Entrada (Entry Points)

### 📄 `main.py` (CLI / Cron)
**Propósito:** Interface de linha de comando para execução de rotinas agendadas (ETL/Batch).

**Análise Técnica:**
*   **Argument Parsing:** Utiliza `argparse` para flexibilidade de execução (`--mode morning` vs `--mode watch`).
*   **Resiliência:** Implementa tratamento de exceções global (`try/except`) com logging estruturado, prevenindo falhas silenciosas em ambientes de orquestração (ex: Airflow).

### 📄 `api.py` (Interface DaaS)
**Propósito:** Exposição dos serviços de risco via API RESTful.

**Stack Específica:**
*   **FastAPI:** Framework assíncrono de alta performance.
*   **Pydantic:** Validação rigorosa de contratos de dados (Input/Output).

**Decisões de Engenharia:**
*   **Endpoint `/v1/risk/execute-batch`:** Atualmente implementado como processamento síncrono para simplicidade do MVP.
*   **Nota de Arquitetura:** Em cenários de alta escala, este endpoint deve evoluir para um padrão *Fire-and-Forget*, enfileirando o processamento (RabbitMQ/Celery) e retornando um `job_id` para consulta posterior (Polling/Webhook).

---

## 3. O Coração do Sistema (Core Logic)

### 📄 `core/pipeline.py` (O Orquestrador)
**Propósito:** Gerenciamento do ciclo de vida da análise de risco (ETL -> Processamento -> Persistência).

**Fluxo de Execução:**
1.  **Ingestão:** Carrega contratos e dados externos (`_load_data`).
2.  **Enriquecimento:** Calcula correlações macroeconômicas.
3.  **Processamento:** Delega o cálculo para estratégias regionais via `RegionalEngineFactory`.
4.  **Persistência:** Salva resultados e métricas de auditoria.

**Padrões de Projeto:**
*   **Factory Pattern:** Utilizado para instanciar dinamicamente a estratégia de risco correta baseada no estado do contrato, garantindo extensibilidade sem modificação do orquestrador.

### 📄 `core/engine.py` (A Máquina de Risco)
**Propósito:** Implementação agnóstica de regras financeiras e matemáticas.

**Lógica Crítica:**
*   **Regra de Veto Climático:** Implementa uma lógica de "Circuit Breaker" no score de crédito. Se `climate_score > 80` (Evento Catastrófico), o histórico financeiro do cliente tem seu peso reduzido drasticamente, priorizando o risco sistêmico.
*   **LTV Dinâmico:** Recalcula o *Loan-to-Value* ajustando o valor da garantia (safra) com base na quebra de produtividade estimada em tempo real.

### 📄 `core/strategies/*.py` (Strategy Pattern)
**Propósito:** Encapsulamento de regras de negócio específicas por região.
*   **`base.py`:** Interface abstrata (ABC) que impõe o contrato de implementação.
*   **`mt_strategy.py`:** Implementa lógica de penalidade logística baseada na distância ao porto e preço do Diesel.
*   **`pr_strategy.py`:** Implementa lógica para regiões com malha logística consolidada.

---

## 4. Camada de Dados e Infraestrutura

### 📄 `core/db.py` (Gerenciador de Conexão)
**Propósito:** Abstração da camada de persistência (Supabase/PostgreSQL).

**Decisões de Engenharia:**
*   **Singleton Manual (`_CLIENT_CACHE`):** Implementado para reutilizar conexões SSL. Essencial em ambientes *serverless* ou scripts de alta frequência para evitar exaustão do *pool* de conexões do banco de dados.

### 📄 `core/climate_risk.py` (Ingestão Assíncrona)
**Propósito:** Cliente HTTP resiliente para dados meteorológicos.

**Engenharia de Resiliência:**
*   **Controle de Concorrência:** Utiliza `asyncio.Semaphore` para limitar requisições simultâneas, respeitando *Rate Limits* de APIs externas.
*   **Fallback Sintético:** Implementa mecanismo de degradação graciosa. Em caso de indisponibilidade das APIs, o sistema utiliza médias históricas para não interromper o pipeline crítico.

### 📄 `core/scout.py` (OSINT & NLP)
**Propósito:** Monitoramento de risco qualitativo via Processamento de Linguagem Natural.
*   **Implementação:** Utiliza modelo Zero-Shot Classification (BART) via Hugging Face API para transformar dados não estruturados (notícias RSS) em vetores de risco estruturados.

---

## 5. Scripts Auxiliares e QA

### 📄 `scripts/seed_portfolio.py`
**Propósito:** Geração de dados sintéticos para testes de carga e validação de hipóteses.
**Metodologia:** Cria clusters de dados intencionais (ex: "Cluster MT Alto Risco" vs "Cluster PR Baixo Risco") para validar a sensibilidade e discriminação do motor de risco.

### 📄 `scripts/run_backtest.py`
**Propósito:** Validação temporal do modelo (Walk-Forward Analysis).
**Importância:** Simula a execução do modelo em janelas de tempo passadas para aferir a acurácia preditiva contra eventos de default reais conhecidos.

---

## 6. Roadmap Técnico & Evolução da Arquitetura

Esta seção detalha os débitos técnicos conhecidos e o plano de evolução para escalar o sistema para um ambiente de produção distribuído.

### 6.1. Débito Técnico Conhecido
*   **Cobertura de Testes:** A cobertura atual foca em testes de integração e backtest. Necessário expandir a suíte de testes unitários (`tests/`) para cobrir *edge cases* matemáticos no `core/engine.py`.
*   **Processamento Síncrono:** A API atual bloqueia a thread durante o cálculo. Em cenários de alta carga, isso pode gerar *timeouts*.

### 6.2. Plano de Evolução (To-Be)
1.  **Arquitetura Orientada a Eventos:**
    *   Desacoplar a API do Motor de Risco utilizando uma fila de mensagens (RabbitMQ ou Redis/Celery).
    *   Transformar o endpoint de execução em assíncrono (*Fire-and-Forget*).

2.  **Containerização Padrão:**
    *   Embora o uso de `Nix` garanta reprodutibilidade, a criação de um `Dockerfile` otimizado (Multi-stage build) é necessária para facilitar o deploy em orquestradores como Kubernetes (K8s) ou ECS.

3.  **Observabilidade:**
    *   Implementar OpenTelemetry para rastreamento distribuído (Tracing) das chamadas entre os módulos de Ingestão, Motor e Banco de Dados.

### 6.3. Padrões de Qualidade (QA)
*   **Type Hinting:** Adoção estrita de tipagem estática verificada via `mypy` no pipeline de CI.
*   **Linting:** Padronização de código via `ruff` ou `black`.

### 6.4. Estratégia de Dados & Fontes (Data Strategy)
*   **Limitações Atuais (MVP):** O sistema utiliza provedores de dados abertos (Open-Meteo, Yahoo Finance) para fins de demonstração e validação de arquitetura. Estes serviços possuem *Rate Limits* restritivos e latência de dados (D+0 com atraso ou D-1).
*   **Requisito para Produção (Enterprise):** A migração para provedores com SLA garantido (ex: Bloomberg, Refinitiv, DTN ou Climatempo) é mandatória para garantir:
    *   Dados de mercado em Tempo Real (Low Latency).
    *   Malha climática de alta resolução (Grids < 5km).
    *   Suporte oficial e estabilidade de API.