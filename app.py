import os
import hashlib
import logging
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify, render_template, session
from werkzeug.utils import secure_filename

# ---------------------------------------------------------------------------
# CONFIGURAÇÃO DE LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    filename="motor_mineirinho.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIGURAÇÕES E PASTAS
# ---------------------------------------------------------------------------
PASTA_ENTRADA     = Path("./caixa_postal_banco")
PASTA_PROCESSADOS = Path("./processados")
PASTA_RELATORIOS  = Path("./relatorios_triade")
PASTA_ERROS       = Path("./erros")
DB_PATH           = Path("motor_seguro.db")

EXTENSOES_VALIDAS = {"csv", "xlsx", "xls"}

for pasta in [PASTA_ENTRADA, PASTA_PROCESSADOS, PASTA_RELATORIOS, PASTA_ERROS]:
    pasta.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# FLASK
# ---------------------------------------------------------------------------
app = Flask(__name__)

# Usa variável de ambiente em produção; fallback local para desenvolvimento
app.secret_key = os.environ.get("SECRET_KEY", "mineirinho-v3-chave-secreta-local")

# ---------------------------------------------------------------------------
# UTILITÁRIO: HASH DE SENHA
# ---------------------------------------------------------------------------

def _hash_senha(senha: str) -> str:
    return hashlib.sha256(senha.encode("utf-8")).hexdigest()

# ---------------------------------------------------------------------------
# SISTEMA DE SEGURANÇA (SQLITE)
# ---------------------------------------------------------------------------

def configurar_seguranca() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS usuarios "
            "(nome TEXT PRIMARY KEY, senha_hash TEXT NOT NULL)"
        )
        # Lê credenciais de variáveis de ambiente (seguro em produção)
        usuario_padrao = os.environ.get("ADMIN_USER", "arthur")
        senha_padrao   = os.environ.get("ADMIN_PASS", "admin123")
        conn.execute(
            "INSERT OR IGNORE INTO usuarios VALUES (?, ?)",
            (usuario_padrao, _hash_senha(senha_padrao)),
        )
        conn.commit()
    log.info("Segurança configurada.")


def validar_acesso(user: str, pw: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT senha_hash FROM usuarios WHERE nome = ?", (user,)
        ).fetchone()
    if row is None:
        return False
    return row[0] == _hash_senha(pw)

# ---------------------------------------------------------------------------
# LEITURA DINÂMICA DE ARQUIVO
# ---------------------------------------------------------------------------

def _ler_arquivo(caminho: Path) -> pd.DataFrame:
    ext = caminho.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(caminho, dtype=str)
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(caminho, dtype=str)
    raise ValueError(f"Extensão não suportada: {ext}")

# ---------------------------------------------------------------------------
# FUNÇÃO DE MINERAÇÃO E MATCH
# ---------------------------------------------------------------------------

def minerar_e_conciliar(caminho: Path) -> dict:
    try:
        dados_arquivo = _ler_arquivo(caminho)
    except Exception as exc:
        log.error("Falha ao ler '%s': %s", caminho.name, exc)
        caminho.rename(PASTA_ERROS / caminho.name)
        return {"erro": str(exc), "arquivo": caminho.name}

    colunas_esperadas = {"id", "valor"}
    if not colunas_esperadas.issubset(dados_arquivo.columns):
        faltando = colunas_esperadas - set(dados_arquivo.columns)
        msg = f"Colunas ausentes: {faltando}"
        log.error(msg)
        caminho.rename(PASTA_ERROS / caminho.name)
        return {"erro": msg, "arquivo": caminho.name}

    dados_arquivo["valor"] = pd.to_numeric(dados_arquivo["valor"], errors="coerce")

    dados_banco = pd.DataFrame({
        "id": dados_arquivo["id"].tolist(),
        "recebido": dados_arquivo["valor"].tolist()
    })

    res = pd.merge(dados_arquivo, dados_banco, on="id", how="left")
    res["status"] = res.apply(
        lambda r: "OK" if r["valor"] == r["recebido"] else "DIVERGENTE", axis=1
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_log = f"log_triade_{timestamp}.csv"
    res.to_csv(PASTA_RELATORIOS / nome_log, index=False)
    log.info("Relatório salvo: %s", nome_log)

    caminho.rename(PASTA_PROCESSADOS / caminho.name)

    registros = res[["id", "valor", "recebido", "status"]].to_dict(orient="records")
    return {
        "arquivo": caminho.name,
        "relatorio": nome_log,
        "registros": registros,
        "total": len(registros),
        "ok": sum(1 for r in registros if r["status"] == "OK"),
        "divergentes": sum(1 for r in registros if r["status"] == "DIVERGENTE"),
    }

# ---------------------------------------------------------------------------
# ROTAS FLASK
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/login", methods=["POST"])
def api_login():
    dados = request.get_json()
    usuario = dados.get("usuario", "").strip()
    senha   = dados.get("senha", "")

    if validar_acesso(usuario, senha):
        session["usuario"] = usuario
        log.info("Usuário '%s' autenticado via web.", usuario)
        return jsonify({"ok": True, "usuario": usuario})

    log.warning("Tentativa de login negada: '%s'.", usuario)
    return jsonify({"ok": False, "erro": "Usuário ou senha incorretos."}), 401


@app.route("/api/logout", methods=["POST"])
def api_logout():
    usuario = session.pop("usuario", None)
    log.info("Usuário '%s' encerrou a sessão.", usuario)
    return jsonify({"ok": True})


@app.route("/api/upload", methods=["POST"])
def api_upload():
    if "usuario" not in session:
        return jsonify({"erro": "Não autenticado."}), 401

    if "arquivo" not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado."}), 400

    arquivo = request.files["arquivo"]
    ext = arquivo.filename.rsplit(".", 1)[-1].lower()

    if ext not in EXTENSOES_VALIDAS:
        return jsonify({"erro": f"Extensão .{ext} não suportada."}), 400

    nome_seguro = secure_filename(arquivo.filename)
    caminho = PASTA_ENTRADA / nome_seguro
    arquivo.save(caminho)

    log.info("Arquivo recebido via web: %s", nome_seguro)
    resultado = minerar_e_conciliar(caminho)

    if "erro" in resultado:
        return jsonify(resultado), 422

    return jsonify(resultado)


@app.route("/api/logs")
def api_logs():
    if "usuario" not in session:
        return jsonify({"erro": "Não autenticado."}), 401
    try:
        with open("motor_mineirinho.log", "r", encoding="utf-8") as f:
            linhas = f.readlines()[-50:]
        return jsonify({"linhas": [l.rstrip() for l in linhas]})
    except FileNotFoundError:
        return jsonify({"linhas": []})


# ---------------------------------------------------------------------------
# INICIALIZAÇÃO (funciona tanto com Gunicorn quanto direto)
# ---------------------------------------------------------------------------

# Garante que o banco é configurado quando o módulo é importado (Gunicorn)
configurar_seguranca()

if __name__ == "__main__":
    log.info("Servidor Flask iniciado.")
    print("\n  ⛏  MINEIRINHO V3.0 — Servidor Web")
    print("  Acesse: http://localhost:5000\n")
    app.run(debug=True, port=5000)
