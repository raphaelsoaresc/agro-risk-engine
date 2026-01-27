from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from core.pipeline import RiskPipeline
from core.context import RiskContext
# Importe seus modelos de dados aqui

app = FastAPI(title="Agro Risk Engine DaaS", version="2.6.0-Projeto")

class SimulationRequest(BaseModel):
    portfolio_id: str
    simulation_tag: str

@app.post("/v1/risk/execute-batch")
async def run_risk_analysis(req: SimulationRequest):
    """
    Endpoint DaaS: O cliente dispara o cálculo e recebe o ID da execução.
    """
    try:
        # Reutiliza sua lógica existente!
        pipeline = RiskPipeline(mode="watch")
        
        # Aqui você adaptaria o pipeline para rodar apenas para o portfolio_id específico
        # Para o PoC, rodar o pipeline padrão e retornar sucesso já valida a integração.
        pipeline.run() 
        
        return {
            "status": "success",
            "message": "Risk Engine executed successfully",
            "data_source": "SANDBOX_YAHOO (Non-Commercial)", # Transparência é chave
            "results_url": f"/v1/results/{req.simulation_tag}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Para rodar: uv run uvicorn api:app --reload