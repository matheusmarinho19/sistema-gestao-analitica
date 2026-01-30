import pandas as pd
from pathlib import Path

# 1) Caminhos  
BASE = Path("C:/Users/Eliane/Downloads/Arquivos Facul/sistema-gestao-analitica/data/raw/olist")
OUT  = Path("C:/Users/Eliane/Downloads/Arquivos Facul/sistema-gestao-analitica/data/operational")
OUT.mkdir(parents=True, exist_ok=True)

ARQ_CUSTOMERS = BASE / "olist_customers_dataset.csv"
ARQ_ORDERS    = BASE / "olist_orders_dataset.csv"
ARQ_PAYMENTS  = BASE / "olist_order_payments_dataset.csv"

# 2) Ler CSVs
customers = pd.read_csv(ARQ_CUSTOMERS)
orders    = pd.read_csv(ARQ_ORDERS, parse_dates=["order_purchase_timestamp"], low_memory=False)
payments  = pd.read_csv(ARQ_PAYMENTS)

# 3) Montar aba "clientes"
# No Olist:
# - customer_id = id "do cliente" (o mesmo cliente pode ter múltiplos pedidos)
# - customer_unique_id = id único do consumidor (melhor para CRM)
# Para seu CRM, é melhor usar customer_unique_id como cliente_id.
clientes = (
    customers[["customer_unique_id", "customer_city", "customer_state"]]
    .drop_duplicates(subset=["customer_unique_id"])
    .rename(columns={
        "customer_unique_id": "cliente_id",
        "customer_city": "cidade",
        "customer_state": "estado"
    })
)

# Criar "nome" fictício (pra ficar apresentável)
clientes["nome"] = ["Cliente " + str(i).zfill(5) for i in range(1, len(clientes) + 1)]

# data_cadastro: vamos inferir pela primeira compra do cliente (primeiro pedido)
orders_customers = orders.merge(
    customers[["customer_id", "customer_unique_id"]],
    on="customer_id",
    how="left"
)

cadastro = (
    orders_customers.groupby("customer_unique_id")["order_purchase_timestamp"]
    .min()
    .reset_index()
    .rename(columns={
        "customer_unique_id": "cliente_id",
        "order_purchase_timestamp": "data_cadastro"
    })
)

clientes = clientes.merge(cadastro, on="cliente_id", how="left")

# 4) Montar aba "vendas"
# Venda = pedido, com valor_total vindo do payments
# Existem pedidos com várias linhas de pagamento, então somamos.
pag_por_pedido = (
    payments.groupby("order_id")["payment_value"]
    .sum()
    .reset_index()
    .rename(columns={"payment_value": "valor_total"})
)

vendas_base = orders.merge(pag_por_pedido, on="order_id", how="left")

# Adicionar cliente_id (unique)
vendas_base = vendas_base.merge(
    customers[["customer_id", "customer_unique_id"]],
    on="customer_id",
    how="left"
)

vendas = (
    vendas_base[["order_id", "customer_unique_id", "order_purchase_timestamp", "valor_total", "order_status"]]
    .rename(columns={
        "order_id": "venda_id",
        "customer_unique_id": "cliente_id",
        "order_purchase_timestamp": "data_venda",
        "order_status": "status_venda"
    })
)

# Se tiver NaN em valor_total (poucos casos), coloca 0
vendas["valor_total"] = vendas["valor_total"].fillna(0)

# 5) Criar aba "funcionarios" (simples, fictícia por enquanto)
funcionarios = pd.DataFrame({
    "funcionario_id": [1,2,3,4,5],
    "nome": ["Ana", "Bruno", "Carla", "Diego", "Ester"],
    "equipe": ["Comercial"]*5,
    "ativo": [1,1,1,1,1]
})

# 6) Criar aba "interacoes" vazia (vamos preencher no Dia 2/3 com regras)
interacoes = pd.DataFrame(columns=[
    "interacao_id",
    "cliente_id",
    "funcionario_id",
    "data_interacao",
    "canal",
    "status",
    "proximo_passo_data",
    "observacao"
])

# 7) Exportar para Excel com múltiplas abas
saida = OUT / "operacao.xlsx"
with pd.ExcelWriter(saida, engine="openpyxl") as writer:
    clientes.to_excel(writer, sheet_name="clientes", index=False)
    funcionarios.to_excel(writer, sheet_name="funcionarios", index=False)
    interacoes.to_excel(writer, sheet_name="interacoes", index=False)
    vendas.to_excel(writer, sheet_name="vendas", index=False)

print(f"OK! Planilha criada em: {saida}")
print(f"Linhas -> clientes: {len(clientes)} | vendas: {len(vendas)}")
