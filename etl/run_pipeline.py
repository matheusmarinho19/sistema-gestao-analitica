import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime

# =========================
# CONFIG (ajuste aqui)
# =========================
USUARIO = "root"
SENHA = "Limites1007@"  
HOST = "127.0.0.1"
PORTA = 3306
BANCO = "siga_pequenas_empresas"
ARQUIVO_EXCEL = "C:/Users/Eliane/Downloads/Arquivos Facul/sistema-gestao-analitica/data/operational/operacao.xlsx"

senha_escapada = quote_plus(SENHA)

engine = create_engine(
    f"mysql+mysqlconnector://{USUARIO}:{senha_escapada}@{HOST}:{PORTA}/{BANCO}"
)

# =========================
# FUNÇÕES AUXILIARES
# =========================
def validar_dados(clientes, funcionarios, vendas):
    # 1) cliente_id não pode ser nulo
    if clientes["cliente_id"].isna().any():
        raise ValueError("clientes: existe cliente_id nulo")

    # 2) cliente_id deve ser único
    if clientes["cliente_id"].duplicated().any():
        raise ValueError("clientes: existe cliente_id duplicado")

    # 3) vendas precisa ter cliente_id
    if vendas["cliente_id"].isna().any():
        raise ValueError("vendas: existe cliente_id nulo")

    # 4) valor_total não pode ser negativo
    if (vendas["valor_total"] < 0).any():
        raise ValueError("vendas: existe valor_total negativo")

def limpar_tabelas(conn):
    conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS = 0;")
    conn.exec_driver_sql("TRUNCATE TABLE vendas;")
    conn.exec_driver_sql("TRUNCATE TABLE interacoes;")
    conn.exec_driver_sql("TRUNCATE TABLE funcionarios;")
    conn.exec_driver_sql("TRUNCATE TABLE clientes;")
    conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS = 1;")

def inserir_log(conn, pipeline, status, lc, lf, lv, msg):
    conn.execute(
        text("""
            INSERT INTO etl_log (pipeline, status, linhas_clientes, linhas_funcionarios, linhas_vendas, mensagem)
            VALUES (:pipeline, :status, :lc, :lf, :lv, :msg)
        """),
        {"pipeline": pipeline, "status": status, "lc": lc, "lf": lf, "lv": lv, "msg": msg}
    )

# =========================
# PIPELINE
# =========================
pipeline_name = "excel_to_mysql_v1"

try:
    # 1) Ler Excel
    clientes = pd.read_excel(ARQUIVO_EXCEL, sheet_name="clientes")
    funcionarios = pd.read_excel(ARQUIVO_EXCEL, sheet_name="funcionarios")
    vendas = pd.read_excel(ARQUIVO_EXCEL, sheet_name="vendas")

    # 2) Ajustes simples de tipos
    clientes["data_cadastro"] = pd.to_datetime(clientes["data_cadastro"]).dt.date
    vendas["data_venda"] = pd.to_datetime(vendas["data_venda"])  # datetime ok
    vendas["valor_total"] = pd.to_numeric(vendas["valor_total"], errors="coerce").fillna(0)

    # 3) Validar
    validar_dados(clientes, funcionarios, vendas)

    with engine.begin() as conn:
        # 4) Limpar tabelas (idempotente)
        limpar_tabelas(conn)

        # 5) Carregar na ordem correta
        clientes.to_sql("clientes", conn, if_exists="append", index=False)
        funcionarios.to_sql("funcionarios", conn, if_exists="append", index=False)
        vendas.to_sql("vendas", conn, if_exists="append", index=False)

        # 6) Log sucesso
        inserir_log(
            conn,
            pipeline=pipeline_name,
            status="SUCESSO",
            lc=len(clientes),
            lf=len(funcionarios),
            lv=len(vendas),
            msg="Carga completa (truncate + reload) OK"
        )

    print("✅ Pipeline executado com sucesso!")

except Exception as e:
    # tenta registrar erro no log
    try:
        with engine.begin() as conn:
            inserir_log(
                conn,
                pipeline=pipeline_name,
                status="ERRO",
                lc=0, lf=0, lv=0,
                msg=str(e)
            )
    except Exception:
        pass

    print("❌ Erro no pipeline:", e)
    raise
