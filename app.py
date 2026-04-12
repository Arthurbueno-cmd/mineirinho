import os
import hashlib
import logging
import sqlite3
import json
import pandas as pd
import pdfplumber
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify, render_template, session
from werkzeug.utils import secure_filename
import anthropic

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
# PASTAS E CONFIGURAÇÕES
# ---------------------------------------------------------------------------
PASTA_UPLOADS     = Path("./uploads")
PASTA_RELATORIOS  = Path("./relatorios_triade")
DB_PATH           = Path("motor_seguro.db")

EXTENSOES_VALIDAS = {"csv", "xlsx", "xls", "pdf"}

for pasta in [PASTA_UPLOADS, PASTA_RELATORIOS]:
    pasta.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# FLASK
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "mineirinho-v3-chave-secreta-local")

# ---------------------------------------------------------------------------
# BANCO DE DADOS
# ---------------------------------------------------------------------------

def configurar_banco() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                nome TEXT PRIMARY KEY,
                senha_hash TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL,
                criado_em TEXT NOT NULL,
                status TEXT DEFAULT 'pendente'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS condominios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lote_id INTEGER NOT NULL,
                nome TEXT NOT NULL,
                arquivo_banco TEXT,
                arquivo_sistema TEXT,
                resultado TEXT,
                analise_ia TEXT,
                status TEXT DEFAULT 'pendente',
                FOREIGN KEY (lote_id) REFERENCES lotes(id)
            )
        """)
        usuario = os.environ.get("ADMIN_USER", "arthur")
        senha   = os.environ.get("ADMIN_PASS", "admin123")
        conn.execute(
            "INSERT OR IGNORE INTO usuarios VALUES (?, ?)",
            (usuario, _hash_senha(senha))
        )
        conn.commit()
    log.info("Banco de dados configurado.")


def _hash_senha(senha: str) -> str:
    return hashlib.sha256(senha.encode("utf-8")).hexdigest()


def validar_acesso(user: str, pw: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT senha_hash FROM usuarios WHERE nome = ?", (user,)
        ).fetchone()
    return row is not None and row[0] == _hash_senha(pw)

# ---------------------------------------------------------------------------
# LEITURA DE ARQUIVOS
# ---------------------------------------------------------------------------

def ler_arquivo(caminho: Path) -> pd.DataFrame:
    ext = caminho.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(caminho, dtype=str)
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(caminho, dtype=str)
    if ext == ".pdf":
        with pdfplumber.open(caminho) as pdf:
            for page in pdf.pages:
                tabela = page.extract_table()
                if tabela:
                    cabecalho = [str(c).strip().lower() for c in tabela[0]]
                    linhas = tabela[1:]
                    return pd.DataFrame(linhas, columns=cabecalho, dtype=str)
        raise ValueError("Nenhuma tabela encontrada no PDF.")
    raise ValueError(f"Extensão não suportada: {ext}")

# ---------------------------------------------------------------------------
# CONCILIAÇÃO
# ---------------------------------------------------------------------------

def conciliar(df_banco: pd.DataFrame, df_sistema: pd.DataFrame) -> list:
    df_banco["valor"]   = pd.to_numeric(df_banco["valor"],   errors="coerce")
    df_sistema["valor"] = pd.to_numeric(df_sistema["valor"], errors="coerce")
    df_banco   = df_banco.rename(columns={"valor": "valor_banco"})
    df_sistema = df_sistema.rename(columns={"valor": "valor_sistema"})
    merged = pd.merge(df_banco, df_sistema, on="id", how="outer")
    registros = []
    for _, row in merged.iterrows():
        vb = row.get("valor_banco")
        vs = row.get("valor_sistema")
        if pd.isna(vb):
            status = "APENAS_SISTEMA"
        elif pd.isna(vs):
            status = "APENAS_BANCO"
        elif abs(float(vb) - float(vs)) < 0.01:
            status = "OK"
        else:
            status = "DIVERGENTE"
        registros.append({
            "id": row["id"],
            "valor_banco":   None if pd.isna(vb) else float(vb),
            "valor_sistema": None if pd.isna(vs) else float(vs),
            "status": status
        })
    return registros

# ---------------------------------------------------------------------------
# ANÁLISE COM IA
# ---------------------------------------------------------------------------

def analisar_com_ia(nome_condominio: str, registros: list) -> str:
    divergencias = [r for r in registros if r["status"] != "OK"]
    if not divergencias:
        return "Nenhuma divergência encontrada. Extrato e sistema estão 100% conciliados."
    resumo = json.dumps(divergencias, ensure_ascii=False, indent=2)
    prompt = f"""Você é um especialista em conciliação financeira de condomínios.

Analise as divergências abaixo encontradas no condomínio "{nome_condominio}" e explique:
1. O que cada divergência significa
2. Qual pode ser a causa provável
3. O que o gestor deve fazer para corrigir

Divergências encontradas:
{resumo}

Seja claro, objetivo e use linguagem simples. Responda em português."""
    try:
        cliente = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        resposta = cliente.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        return resposta.content[0].text
    except Exception as e:
        log.error("Erro na análise IA: %s", e)
        return f"Análise IA indisponível: {str(e)}"

# ---------------------------------------------------------------------------
# ROTAS — AUTENTICAÇÃO
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/login", methods=["POST"])
def api_login():
    dados   = request.get_json()
    usuario = dados.get("usuario", "").strip()
    senha   = dados.get("senha", "")
    if validar_acesso(usuario, senha):
        session["usuario"] = usuario
        return jsonify({"ok": True, "usuario": usuario})
    return jsonify({"ok": False, "erro": "Usuário ou senha incorretos."}), 401

@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.pop("usuario", None)
    return jsonify({"ok": True})

# ---------------------------------------------------------------------------
# ROTAS — LOTES
# ---------------------------------------------------------------------------

@app.route("/api/lotes", methods=["GET"])
def listar_lotes():
    if "usuario" not in session:
        return jsonify({"erro": "Não autenticado."}), 401
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        lotes = conn.execute("SELECT * FROM lotes ORDER BY criado_em DESC").fetchall()
        resultado = []
        for lote in lotes:
            condominios = conn.execute(
                "SELECT id, nome, status, analise_ia FROM condominios WHERE lote_id = ?",
                (lote["id"],)
            ).fetchall()
            resultado.append({
                "id": lote["id"],
                "nome": lote["nome"],
                "criado_em": lote["criado_em"],
                "status": lote["status"],
                "condominios": [dict(c) for c in condominios]
            })
    return jsonify(resultado)

@app.route("/api/lotes", methods=["POST"])
def criar_lote():
    if "usuario" not in session:
        return jsonify({"erro": "Não autenticado."}), 401
    dados = request.get_json()
    nome  = dados.get("nome", "").strip()
    if not nome:
        return jsonify({"erro": "Nome do lote é obrigatório."}), 400
    criado_em = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            "INSERT INTO lotes (nome, criado_em) VALUES (?, ?)", (nome, criado_em)
        )
        lote_id = cursor.lastrowid
        conn.commit()
    return jsonify({"ok": True, "id": lote_id, "nome": nome})

@app.route("/api/lotes/<int:lote_id>/condominios", methods=["POST"])
def adicionar_condominio(lote_id):
    if "usuario" not in session:
        return jsonify({"erro": "Não autenticado."}), 401
    nome = request.form.get("nome", "").strip()
    if not nome:
        return jsonify({"erro": "Nome do condomínio é obrigatório."}), 400
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            "INSERT INTO condominios (lote_id, nome) VALUES (?, ?)", (lote_id, nome)
        )
        cond_id = cursor.lastrowid
        conn.commit()
    return jsonify({"ok": True, "id": cond_id, "nome": nome})

@app.route("/api/condominios/<int:cond_id>/upload", methods=["POST"])
def upload_condominio(cond_id):
    if "usuario" not in session:
        return jsonify({"erro": "Não autenticado."}), 401
    if "arquivo_banco" not in request.files or "arquivo_sistema" not in request.files:
        return jsonify({"erro": "Envie os dois arquivos."}), 400
    def salvar(arquivo):
        ext = arquivo.filename.rsplit(".", 1)[-1].lower()
        if ext not in EXTENSOES_VALIDAS:
            raise ValueError(f"Extensão .{ext} não suportada.")
        nome = f"{cond_id}_{secure_filename(arquivo.filename)}"
        caminho = PASTA_UPLOADS / nome
        arquivo.save(caminho)
        return str(caminho)
    try:
        caminho_banco   = salvar(request.files["arquivo_banco"])
        caminho_sistema = salvar(request.files["arquivo_sistema"])
    except ValueError as e:
        return jsonify({"erro": str(e)}), 400
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE condominios SET arquivo_banco=?, arquivo_sistema=?, status='aguardando' WHERE id=?",
            (caminho_banco, caminho_sistema, cond_id)
        )
        conn.commit()
    return jsonify({"ok": True, "mensagem": "Arquivos recebidos."})

@app.route("/api/lotes/<int:lote_id>/processar", methods=["POST"])
def processar_lote(lote_id):
    if "usuario" not in session:
        return jsonify({"erro": "Não autenticado."}), 401
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        condominios = conn.execute(
            "SELECT * FROM condominios WHERE lote_id = ? AND status = 'aguardando'", (lote_id,)
        ).fetchall()
    if not condominios:
        return jsonify({"erro": "Nenhum condomínio pronto para processar."}), 400
    resultados = []
    for cond in condominios:
        try:
            df_banco   = ler_arquivo(Path(cond["arquivo_banco"]))
            df_sistema = ler_arquivo(Path(cond["arquivo_sistema"]))
            registros  = conciliar(df_banco, df_sistema)
            analise    = analisar_com_ia(cond["nome"], registros)
            total       = len(registros)
            ok          = sum(1 for r in registros if r["status"] == "OK")
            divergentes = total - ok
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "UPDATE condominios SET resultado=?, analise_ia=?, status='concluido' WHERE id=?",
                    (json.dumps(registros), analise, cond["id"])
                )
                conn.commit()
            resultados.append({
                "condominio": cond["nome"],
                "total": total, "ok": ok, "divergentes": divergentes,
                "analise_ia": analise, "registros": registros
            })
        except Exception as e:
            log.error("Erro ao processar '%s': %s", cond["nome"], e)
            resultados.append({"condominio": cond["nome"], "erro": str(e)})
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE lotes SET status='concluido' WHERE id=?", (lote_id,))
        conn.commit()
    return jsonify({"ok": True, "resultados": resultados})

@app.route("/api/condominios/<int:cond_id>", methods=["GET"])
def detalhe_condominio(cond_id):
    if "usuario" not in session:
        return jsonify({"erro": "Não autenticado."}), 401
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cond = conn.execute("SELECT * FROM condominios WHERE id=?", (cond_id,)).fetchone()
    if not cond:
        return jsonify({"erro": "Não encontrado."}), 404
    return jsonify({
        "id": cond["id"], "nome": cond["nome"], "status": cond["status"],
        "analise_ia": cond["analise_ia"],
        "registros": json.loads(cond["resultado"]) if cond["resultado"] else []
    })

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
# INICIALIZAÇÃO
# ---------------------------------------------------------------------------
configurar_banco()

if __name__ == "__main__":
    print("\n  ⛏  MINEIRINHO V3.0 — Conciliação Inteligente")
    print("  Acesse: http://localhost:5000\n")
    app.run(debug=True, port=5000)
