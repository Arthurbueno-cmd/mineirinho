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

logging.basicConfig(
    filename="motor_mineirinho.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

PASTA_UPLOADS    = Path("./uploads")
PASTA_RELATORIOS = Path("./relatorios_triade")
DB_PATH          = Path("motor_seguro.db")
EXTENSOES_VALIDAS = {"csv", "xlsx", "xls", "pdf"}

for pasta in [PASTA_UPLOADS, PASTA_RELATORIOS]:
    pasta.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "mineirinho-v3-chave-secreta-local")

# ---------------------------------------------------------------------------
# BANCO DE DADOS
# ---------------------------------------------------------------------------

def configurar_banco() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        # Usuários com papel (admin / cliente / funcionario)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT UNIQUE NOT NULL,
                senha_hash TEXT NOT NULL,
                papel TEXT DEFAULT 'cliente',
                dono_id INTEGER,
                criado_em TEXT
            )
        """)
        # Lotes vinculados a um usuário
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL,
                criado_em TEXT NOT NULL,
                status TEXT DEFAULT 'pendente',
                usuario_id INTEGER NOT NULL
            )
        """)
        # Condomínios
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

        # Admin padrão
        admin = os.environ.get("ADMIN_USER", "arthur")
        senha = os.environ.get("ADMIN_PASS", "admin123")
        agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT OR IGNORE INTO usuarios (nome, senha_hash, papel, criado_em) VALUES (?,?,?,?)",
            (admin, _hash(senha), "admin", agora)
        )
        conn.commit()
    log.info("Banco configurado.")


def _hash(senha: str) -> str:
    return hashlib.sha256(senha.encode()).hexdigest()


def get_usuario(nome: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute("SELECT * FROM usuarios WHERE nome=?", (nome,)).fetchone()


def validar_acesso(nome: str, senha: str) -> bool:
    u = get_usuario(nome)
    return u is not None and u["senha_hash"] == _hash(senha)


def usuario_logado():
    """Retorna o usuário da sessão ou None."""
    nome = session.get("usuario")
    if not nome:
        return None
    return get_usuario(nome)


def ids_visiveis(u) -> list:
    """
    Retorna lista de usuario_ids cujos lotes o usuário pode ver.
    - admin: todos
    - cliente: só ele e seus funcionários
    - funcionario: só o dono (cliente) e outros funcionários do mesmo dono
    """
    with sqlite3.connect(DB_PATH) as conn:
        if u["papel"] == "admin":
            rows = conn.execute("SELECT id FROM usuarios").fetchall()
            return [r[0] for r in rows]
        dono_id = u["id"] if u["papel"] == "cliente" else u["dono_id"]
        rows = conn.execute(
            "SELECT id FROM usuarios WHERE id=? OR dono_id=?", (dono_id, dono_id)
        ).fetchall()
        return [r[0] for r in rows]

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
                    cab = [str(c).strip().lower() for c in tabela[0]]
                    return pd.DataFrame(tabela[1:], columns=cab, dtype=str)
        raise ValueError("Nenhuma tabela encontrada no PDF.")
    raise ValueError(f"Extensão não suportada: {ext}")

# ---------------------------------------------------------------------------
# CONCILIAÇÃO
# ---------------------------------------------------------------------------

def conciliar(df_banco, df_sistema) -> list:
    df_banco["valor"]   = pd.to_numeric(df_banco["valor"],   errors="coerce")
    df_sistema["valor"] = pd.to_numeric(df_sistema["valor"], errors="coerce")
    df_banco   = df_banco.rename(columns={"valor": "valor_banco"})
    df_sistema = df_sistema.rename(columns={"valor": "valor_sistema"})
    merged = pd.merge(df_banco, df_sistema, on="id", how="outer")
    registros = []
    for _, row in merged.iterrows():
        vb, vs = row.get("valor_banco"), row.get("valor_sistema")
        if pd.isna(vb):       status = "APENAS_SISTEMA"
        elif pd.isna(vs):     status = "APENAS_BANCO"
        elif abs(float(vb)-float(vs)) < 0.01: status = "OK"
        else:                 status = "DIVERGENTE"
        registros.append({
            "id": row["id"],
            "valor_banco":   None if pd.isna(vb) else float(vb),
            "valor_sistema": None if pd.isna(vs) else float(vs),
            "status": status
        })
    return registros

# ---------------------------------------------------------------------------
# IA
# ---------------------------------------------------------------------------

def analisar_com_ia(nome_cond: str, registros: list) -> str:
    divs = [r for r in registros if r["status"] != "OK"]
    if not divs:
        return "Nenhuma divergência encontrada. Extrato e sistema estão 100% conciliados."
    prompt = f"""Você é especialista em conciliação financeira de condomínios.
Analise as divergências do condomínio "{nome_cond}" e explique:
1. O que cada divergência significa
2. Causa provável
3. Como o gestor deve corrigir

Divergências:
{json.dumps(divs, ensure_ascii=False, indent=2)}

Seja claro e objetivo. Responda em português."""
    try:
        cliente = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY",""))
        r = cliente.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=1000,
            messages=[{"role":"user","content":prompt}]
        )
        return r.content[0].text
    except Exception as e:
        log.error("Erro IA: %s", e)
        return f"Análise IA indisponível: {e}"

# ---------------------------------------------------------------------------
# ROTAS — AUTH
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/login", methods=["POST"])
def api_login():
    d = request.get_json()
    nome, senha = d.get("usuario","").strip(), d.get("senha","")
    if validar_acesso(nome, senha):
        u = get_usuario(nome)
        session["usuario"] = nome
        return jsonify({"ok":True,"usuario":nome,"papel":u["papel"]})
    return jsonify({"ok":False,"erro":"Usuário ou senha incorretos."}), 401

@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.pop("usuario", None)
    return jsonify({"ok":True})

@app.route("/api/me")
def api_me():
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401
    return jsonify({"nome":u["nome"],"papel":u["papel"]})

# ---------------------------------------------------------------------------
# ROTAS — SENHA
# ---------------------------------------------------------------------------

@app.route("/api/trocar-senha", methods=["POST"])
def trocar_senha():
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401
    d = request.get_json()
    atual = d.get("senha_atual","")
    nova  = d.get("senha_nova","").strip()
    if u["senha_hash"] != _hash(atual):
        return jsonify({"ok":False,"erro":"Senha atual incorreta."}), 400
    if len(nova) < 6:
        return jsonify({"ok":False,"erro":"A nova senha deve ter ao menos 6 caracteres."}), 400
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE usuarios SET senha_hash=? WHERE id=?", (_hash(nova), u["id"]))
        conn.commit()
    log.info("Senha alterada: %s", u["nome"])
    return jsonify({"ok":True})

# ---------------------------------------------------------------------------
# ROTAS — USUÁRIOS (admin e clientes)
# ---------------------------------------------------------------------------

@app.route("/api/usuarios", methods=["GET"])
def listar_usuarios():
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        if u["papel"] == "admin":
            # Admin vê todos
            rows = conn.execute("SELECT id,nome,papel,criado_em,dono_id FROM usuarios ORDER BY criado_em DESC").fetchall()
        else:
            # Cliente vê apenas seus funcionários
            rows = conn.execute(
                "SELECT id,nome,papel,criado_em,dono_id FROM usuarios WHERE dono_id=?",
                (u["id"],)
            ).fetchall()

    return jsonify([dict(r) for r in rows])


@app.route("/api/usuarios", methods=["POST"])
def criar_usuario():
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401
    if u["papel"] not in ("admin","cliente"):
        return jsonify({"erro":"Sem permissão."}), 403

    d     = request.get_json()
    nome  = d.get("nome","").strip()
    senha = d.get("senha","").strip()
    papel = d.get("papel","cliente")

    if not nome or not senha:
        return jsonify({"ok":False,"erro":"Nome e senha são obrigatórios."}), 400
    if len(senha) < 6:
        return jsonify({"ok":False,"erro":"Senha deve ter ao menos 6 caracteres."}), 400

    # Admin pode criar clientes; cliente só pode criar funcionários
    if u["papel"] == "cliente":
        papel = "funcionario"

    # Define o dono
    dono_id = None
    if papel == "funcionario":
        dono_id = u["id"] if u["papel"] == "cliente" else None

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO usuarios (nome, senha_hash, papel, dono_id, criado_em) VALUES (?,?,?,?,?)",
                (nome, _hash(senha), papel, dono_id, agora)
            )
            conn.commit()
        log.info("Usuário criado: %s (%s)", nome, papel)
        return jsonify({"ok":True,"nome":nome,"papel":papel})
    except sqlite3.IntegrityError:
        return jsonify({"ok":False,"erro":"Usuário já existe."}), 409


@app.route("/api/usuarios/<int:uid>", methods=["DELETE"])
def remover_usuario(uid):
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        alvo = conn.execute("SELECT * FROM usuarios WHERE id=?", (uid,)).fetchone()

    if not alvo:
        return jsonify({"erro":"Usuário não encontrado."}), 404
    if alvo["papel"] == "admin":
        return jsonify({"erro":"Não é possível remover o admin."}), 403
    if u["papel"] == "cliente" and alvo["dono_id"] != u["id"]:
        return jsonify({"erro":"Sem permissão."}), 403

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM usuarios WHERE id=?", (uid,))
        conn.commit()
    log.info("Usuário removido: id=%s", uid)
    return jsonify({"ok":True})

# ---------------------------------------------------------------------------
# ROTAS — LOTES
# ---------------------------------------------------------------------------

@app.route("/api/lotes", methods=["GET"])
def listar_lotes():
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401

    ids = ids_visiveis(u)
    placeholders = ",".join("?" * len(ids))

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        lotes = conn.execute(
            f"SELECT * FROM lotes WHERE usuario_id IN ({placeholders}) ORDER BY criado_em DESC", ids
        ).fetchall()
        resultado = []
        for lote in lotes:
            conds = conn.execute(
                "SELECT id,nome,status FROM condominios WHERE lote_id=?", (lote["id"],)
            ).fetchall()
            resultado.append({
                "id": lote["id"], "nome": lote["nome"],
                "criado_em": lote["criado_em"], "status": lote["status"],
                "condominios": [dict(c) for c in conds]
            })
    return jsonify(resultado)


@app.route("/api/lotes", methods=["POST"])
def criar_lote():
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401
    d    = request.get_json()
    nome = d.get("nome","").strip()
    if not nome:
        return jsonify({"erro":"Nome obrigatório."}), 400

    # Funcionário cria lote em nome do dono (cliente)
    uid = u["dono_id"] if u["papel"] == "funcionario" else u["id"]
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT INTO lotes (nome, criado_em, usuario_id) VALUES (?,?,?)", (nome, agora, uid)
        )
        lote_id = cur.lastrowid
        conn.commit()
    return jsonify({"ok":True,"id":lote_id,"nome":nome})


@app.route("/api/lotes/<int:lote_id>/condominios", methods=["POST"])
def adicionar_condominio(lote_id):
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401
    nome = request.form.get("nome","").strip()
    if not nome:
        return jsonify({"erro":"Nome obrigatório."}), 400
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("INSERT INTO condominios (lote_id,nome) VALUES (?,?)", (lote_id, nome))
        cond_id = cur.lastrowid
        conn.commit()
    return jsonify({"ok":True,"id":cond_id,"nome":nome})


@app.route("/api/condominios/<int:cond_id>/upload", methods=["POST"])
def upload_condominio(cond_id):
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401
    if "arquivo_banco" not in request.files or "arquivo_sistema" not in request.files:
        return jsonify({"erro":"Envie os dois arquivos."}), 400

    def salvar(arq):
        ext = arq.filename.rsplit(".",1)[-1].lower()
        if ext not in EXTENSOES_VALIDAS:
            raise ValueError(f"Extensão .{ext} não suportada.")
        nome = f"{cond_id}_{secure_filename(arq.filename)}"
        p = PASTA_UPLOADS / nome
        arq.save(p)
        return str(p)

    try:
        cb = salvar(request.files["arquivo_banco"])
        cs = salvar(request.files["arquivo_sistema"])
    except ValueError as e:
        return jsonify({"erro":str(e)}), 400

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE condominios SET arquivo_banco=?,arquivo_sistema=?,status='aguardando' WHERE id=?",
            (cb, cs, cond_id)
        )
        conn.commit()
    return jsonify({"ok":True})


@app.route("/api/lotes/<int:lote_id>/processar", methods=["POST"])
def processar_lote(lote_id):
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        conds = conn.execute(
            "SELECT * FROM condominios WHERE lote_id=? AND status='aguardando'", (lote_id,)
        ).fetchall()

    if not conds:
        return jsonify({"erro":"Nenhum condomínio pronto."}), 400

    resultados = []
    for cond in conds:
        try:
            df_b = ler_arquivo(Path(cond["arquivo_banco"]))
            df_s = ler_arquivo(Path(cond["arquivo_sistema"]))
            regs = conciliar(df_b, df_s)
            analise = analisar_com_ia(cond["nome"], regs)
            ok  = sum(1 for r in regs if r["status"]=="OK")
            div = len(regs) - ok
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "UPDATE condominios SET resultado=?,analise_ia=?,status='concluido' WHERE id=?",
                    (json.dumps(regs), analise, cond["id"])
                )
                conn.commit()
            resultados.append({"condominio":cond["nome"],"total":len(regs),"ok":ok,"divergentes":div,"analise_ia":analise,"registros":regs})
        except Exception as e:
            log.error("Erro '%s': %s", cond["nome"], e)
            resultados.append({"condominio":cond["nome"],"erro":str(e)})

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE lotes SET status='concluido' WHERE id=?", (lote_id,))
        conn.commit()
    return jsonify({"ok":True,"resultados":resultados})


@app.route("/api/condominios/<int:cond_id>", methods=["GET"])
def detalhe_condominio(cond_id):
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cond = conn.execute("SELECT * FROM condominios WHERE id=?", (cond_id,)).fetchone()
    if not cond:
        return jsonify({"erro":"Não encontrado."}), 404
    return jsonify({
        "id":cond["id"],"nome":cond["nome"],"status":cond["status"],
        "analise_ia":cond["analise_ia"],
        "registros":json.loads(cond["resultado"]) if cond["resultado"] else []
    })


@app.route("/api/logs")
def api_logs():
    u = usuario_logado()
    if not u:
        return jsonify({"erro":"Não autenticado."}), 401
    try:
        with open("motor_mineirinho.log","r",encoding="utf-8") as f:
            linhas = f.readlines()[-50:]
        return jsonify({"linhas":[l.rstrip() for l in linhas]})
    except FileNotFoundError:
        return jsonify({"linhas":[]})


# ---------------------------------------------------------------------------
configurar_banco()

if __name__ == "__main__":
    print("\n  ⛏  MINEIRINHO V3.0\n  http://localhost:5000\n")
    app.run(debug=True, port=5000)