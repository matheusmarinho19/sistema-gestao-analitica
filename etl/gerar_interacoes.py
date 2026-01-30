import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import timedelta

# =========================
# CONFIG (ajuste se precisar)
# =========================
USUARIO = "root"
SENHA = "Limites1007@"   
HOST = "127.0.0.1"
PORTA = 3306
BANCO = "siga_pequenas_empresas"

senha_escapada = quote_plus(SENHA)
engine = create_engine(
    f"mysql+mysqlconnector://{USUARIO}:{senha_escapada}@{HOST}:{PORTA}/{BANCO}"
)

# Para não ficar gigante, vamos gerar interações para uma amostra de vendas
LIMITE_VENDAS = 5000  # pode aumentar depois

with engine.connect() as conn:
    vendas = pd.read_sql(f"""
        SELECT venda_id, cliente_id, data_venda, valor_total, status_venda
        FROM vendas
        ORDER BY data_venda
        LIMIT {LIMITE_VENDAS}
    """, conn)

    funcionarios = pd.read_sql("SELECT funcionario_id FROM funcionarios WHERE ativo=1", conn)

# Distribuir responsável aleatoriamente (simula equipe comercial)
rng = np.random.default_rng(42)
vendas["funcionario_id"] = rng.choice(funcionarios["funcionario_id"].values, size=len(vendas), replace=True)

# Criar regras de interação
def definir_status(row):
    st = str(row["status_venda"]).lower()
    if st in ["canceled", "unavailable"]:
        return "recuperacao"
    return "pos_venda"

def definir_canal(row):
    # canal simples baseado em valor_total
    if row["valor_total"] >= 300:
        return "ligacao"
    return "whatsapp"

interacoes = pd.DataFrame()
interacoes["cliente_id"] = vendas["cliente_id"]
interacoes["funcionario_id"] = vendas["funcionario_id"]
interacoes["data_interacao"] = pd.to_datetime(vendas["data_venda"]) + pd.to_timedelta(rng.integers(0, 3, size=len(vendas)), unit="D")
interacoes["status"] = vendas.apply(definir_status, axis=1)
interacoes["canal"] = vendas.apply(definir_canal, axis=1)

# Próximo passo: 2 a 7 dias depois (alguns atrasados de propósito)
dias_follow = rng.integers(2, 8, size=len(vendas))
interacoes["proximo_passo_data"] = (pd.to_datetime(interacoes["data_interacao"]) + pd.to_timedelta(dias_follow, unit="D")).dt.date

# Criar observações
interacoes["observacao"] = np.where(
    vendas["valor_total"] >= 300,
    "Cliente de maior valor: acompanhar satisfação e upsell",
    "Contato padrão pós-venda"
)

# Gerar uma parcela de follow-ups atrasados (para aparecer no BI)
idx_atraso = rng.choice(interacoes.index, size=int(len(interacoes) * 0.12), replace=False)
interacoes.loc[idx_atraso, "proximo_passo_data"] = (
    pd.to_datetime(interacoes.loc[idx_atraso, "data_interacao"]) - timedelta(days=3)
).dt.date

# Carregar no MySQL (limpa e recria interacoes)
with engine.begin() as conn:
    conn.exec_driver_sql("TRUNCATE TABLE interacoes;")
    interacoes.to_sql("interacoes", conn, if_exists="append", index=False)

print(f"✅ Interações geradas e carregadas: {len(interacoes)} linhas")
