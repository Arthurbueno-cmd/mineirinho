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
from groq import Groq

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

@app.errorhandler(Exception)
def handle_exception(e):
    log.error("Erro não tratado: %s", e, exc_info=True)
    return jsonify({"erro": str(e)}), 500

# ---------------------------------------------------------------------------
# BANCO DE DADOS
# ---------------------------------------------------------------------------

def configurar_banco() -> None:
    """Configura o banco verificando e migrando schema se necessário.""" 
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
        # Migração: adiciona colunas novas se banco legado não tiver
        colunas = [r[1] for r in conn.execute("PRAGMA table_info(usuarios)").fetchall()]
        if "papel" not in colunas:
            conn.execute("ALTER TABLE usuarios ADD COLUMN papel TEXT DEFAULT 'cliente'")
        if "dono_id" not in colunas:
            conn.execute("ALTER TABLE usuarios ADD COLUMN dono_id INTEGER")
        if "criado_em" not in colunas:
            conn.execute("ALTER TABLE usuarios ADD COLUMN criado_em TEXT")
        # Garante que o admin sempre tem papel correto
        admin = os.environ.get("ADMIN_USER", "arthur")
        conn.execute("UPDATE usuarios SET papel='admin' WHERE nome=?", (admin,))

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
        # Migração: adiciona usuario_id na tabela lotes se não existir
        cols_lotes = [r[1] for r in conn.execute("PRAGMA table_info(lotes)").fetchall()]
        if "usuario_id" not in cols_lotes:
            conn.execute("ALTER TABLE lotes ADD COLUMN usuario_id INTEGER DEFAULT 1")

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

def _limpar_valor(v) -> float:
    """Converte string brasileira de valor para float. Ex: '1.199,00' -> 1199.0"""
    if v is None or str(v).strip() in ('', '-', 'None', 'nan'):
        return 0.0
    s = str(v).strip()
    # Remove R$, espaços e pontos de milhar, troca vírgula por ponto
    s = s.replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.')
    try:
        return abs(float(s))
    except:
        return 0.0


def _detectar_formato(df: pd.DataFrame) -> str:
    """
    Detecta se o DataFrame é formato simples (id+valor) ou extrato real (data+descrição+valores).
    Retorna 'simples' ou 'extrato'.
    """
    colunas = [c.lower().strip() for c in df.columns]
    if 'id' in colunas and 'valor' in colunas:
        return 'simples'
    # Detecta colunas de extrato bancário ou relatório de condomínio
    tem_data = any('data' in c for c in colunas)
    tem_valor = any(c in colunas for c in ['crédito (r$)', 'credito', 'r$ receita', 'receita', 'débito (r$)', 'debito', 'r$ despesa', 'despesa'])
    if tem_data and tem_valor:
        return 'extrato'
    return 'simples'


def _normalizar_extrato(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza um DataFrame de extrato real para o formato padrão:
    id (data+seq), descricao, valor_entrada, valor_saida, valor_liquido
    """
    colunas = {c.lower().strip(): c for c in df.columns}
    
    # Encontra coluna de data
    col_data = next((colunas[c] for c in colunas if 'data' in c), None)
    
    # Encontra coluna de descrição
    col_desc = next((colunas[c] for c in colunas if any(x in c for x in ['descrição', 'descricao', 'lançamento', 'lancamento', 'histórico', 'historico'])), None)
    
    # Encontra colunas de valores
    col_credito = next((colunas[c] for c in colunas if any(x in c for x in ['crédito', 'credito', 'receita', 'entrada'])), None)
    col_debito  = next((colunas[c] for c in colunas if any(x in c for x in ['débito', 'debito', 'despesa', 'saída', 'saida'])), None)

    rows = []
    contador = {}
    for _, row in df.iterrows():
        data = str(row[col_data]).strip() if col_data else ''
        desc = str(row[col_desc]).strip() if col_desc else ''
        
        # Ignora linhas vazias, totais e saldos
        if not data or data.lower() in ('none', 'nan', '', 'data') or            any(x in desc.lower() for x in ['total', 'saldo anterior', 'saldo invest', 'saldo']):
            continue
        
        entrada = _limpar_valor(row[col_credito]) if col_credito else 0.0
        saida   = _limpar_valor(row[col_debito])  if col_debito  else 0.0
        liquido = entrada - saida

        # Gera ID único: data + contador
        contador[data] = contador.get(data, 0) + 1
        id_unico = f"{data}_{contador[data]:02d}"

        rows.append({
            'id': id_unico,
            'data': data,
            'descricao': desc,
            'valor_entrada': entrada,
            'valor_saida': saida,
            'valor': liquido
        })

    return pd.DataFrame(rows)


def ler_arquivo(caminho: Path) -> pd.DataFrame:
    """
    Lê arquivo CSV, XLSX ou PDF e retorna DataFrame normalizado.
    
    Suporta dois formatos:
    - Simples: colunas 'id' e 'valor' (CSV/XLSX manuais)
    - Extrato: formato real de banco ou sistema de condomínio com data, descrição, crédito/débito
    """
    ext = caminho.suffix.lower()

    if ext == ".csv":
        df = pd.read_csv(caminho, dtype=str)
    elif ext in {".xlsx", ".xls"}:
        df = pd.read_excel(caminho, dtype=str)
    elif ext == ".pdf":
        todas_linhas = []
        cabecalho = None
        with pdfplumber.open(caminho) as pdf:
            for page in pdf.pages:
                tabela = page.extract_table()
                if tabela:
                    if cabecalho is None:
                        cabecalho = [str(c).strip() if c else '' for c in tabela[0]]
                        todas_linhas.extend(tabela[1:])
                    else:
                        # Continua tabela em páginas seguintes
                        for row in tabela:
                            if row and str(row[0]).strip() not in (cabecalho[0], '', 'None'):
                                todas_linhas.append(row)
        if not cabecalho:
            raise ValueError("Nenhuma tabela encontrada no PDF.")
        df = pd.DataFrame(todas_linhas, columns=cabecalho, dtype=str)
    else:
        raise ValueError(f"Extensão não suportada: {ext}")

    # Detecta formato e normaliza
    fmt = _detectar_formato(df)
    if fmt == 'extrato':
        return _normalizar_extrato(df)
    
    # Formato simples — garante que valor é numérico
    df['valor'] = df['valor'].apply(_limpar_valor)
    return df

# ---------------------------------------------------------------------------
# CONCILIAÇÃO
# ---------------------------------------------------------------------------

def conciliar(df_banco, df_sistema) -> list:
    """
    Concilia dois DataFrames inteligentemente.
    
    Se ambos têm formato de extrato (com data e descrição):
      - Cruza por data + valor líquido
      - Gera ID automático baseado na data
    
    Se formato simples (id + valor):
      - Cruza pelo campo 'id' diretamente
    
    Sempre retorna lista com: id, descricao, valor_banco, valor_sistema, status
    """
    tem_extrato_banco  = 'data' in df_banco.columns and 'descricao' in df_banco.columns
    tem_extrato_sistema = 'data' in df_sistema.columns and 'descricao' in df_sistema.columns

    if tem_extrato_banco and tem_extrato_sistema:
        return _conciliar_extrato(df_banco, df_sistema)
    else:
        return _conciliar_simples(df_banco, df_sistema)


def _conciliar_simples(df_banco, df_sistema) -> list:
    """Conciliação por campo 'id' — formato CSV/XLSX simples."""
    df_banco["valor"]   = pd.to_numeric(df_banco["valor"], errors="coerce")
    df_sistema["valor"] = pd.to_numeric(df_sistema["valor"], errors="coerce")
    df_banco   = df_banco.rename(columns={"valor": "valor_banco"})
    df_sistema = df_sistema.rename(columns={"valor": "valor_sistema"})
    merged = pd.merge(df_banco[['id','valor_banco']], df_sistema[['id','valor_sistema']], on="id", how="outer")
    registros = []
    for _, row in merged.iterrows():
        vb, vs = row.get("valor_banco"), row.get("valor_sistema")
        if pd.isna(vb):                        status = "APENAS_SISTEMA"
        elif pd.isna(vs):                      status = "APENAS_BANCO"
        elif abs(float(vb)-float(vs)) < 0.01: status = "OK"
        else:                                  status = "DIVERGENTE"
        registros.append({
            "id": row["id"],
            "descricao": "",
            "valor_banco":   None if pd.isna(vb) else float(vb),
            "valor_sistema": None if pd.isna(vs) else float(vs),
            "status": status
        })
    return registros


def _conciliar_extrato(df_banco, df_sistema) -> list:
    """
    Conciliação de extratos reais por data + valor.
    Agrupa lançamentos do mesmo dia e compara totais.
    Lançamentos sem par são marcados como APENAS_BANCO ou APENAS_SISTEMA.
    """
    registros = []

    # Agrupa banco por data
    banco_por_data = {}
    for _, row in df_banco.iterrows():
        data = str(row.get('data', '')).strip()
        if not data or data == 'nan':
            continue
        if data not in banco_por_data:
            banco_por_data[data] = []
        banco_por_data[data].append({
            'id': row.get('id', ''),
            'descricao': str(row.get('descricao', '')),
            'valor': float(row.get('valor', 0) or 0),
            'entrada': float(row.get('valor_entrada', 0) or 0),
            'saida': float(row.get('valor_saida', 0) or 0),
        })

    # Agrupa sistema por data
    sistema_por_data = {}
    for _, row in df_sistema.iterrows():
        data = str(row.get('data', '')).strip()
        if not data or data == 'nan':
            continue
        if data not in sistema_por_data:
            sistema_por_data[data] = []
        sistema_por_data[data].append({
            'id': row.get('id', ''),
            'descricao': str(row.get('descricao', '')),
            'valor': float(row.get('valor', 0) or 0),
            'entrada': float(row.get('valor_entrada', 0) or 0),
            'saida': float(row.get('valor_saida', 0) or 0),
        })

    todas_datas = sorted(set(list(banco_por_data.keys()) + list(sistema_por_data.keys())))

    for data in todas_datas:
        itens_banco   = banco_por_data.get(data, [])
        itens_sistema = sistema_por_data.get(data, [])

        # Tenta casar lançamentos por valor
        sistema_usados = [False] * len(itens_sistema)

        for item_b in itens_banco:
            casado = False
            for i, item_s in enumerate(itens_sistema):
                if not sistema_usados[i] and abs(item_b['valor'] - item_s['valor']) < 0.05:
                    # Encontrou par
                    sistema_usados[i] = True
                    casado = True
                    status = "OK" if abs(item_b['entrada'] - item_s['entrada']) < 0.05 and                                      abs(item_b['saida'] - item_s['saida']) < 0.05 else "DIVERGENTE"
                    registros.append({
                        "id": item_b['id'],
                        "descricao": item_b['descricao'] or item_s['descricao'],
                        "valor_banco":   item_b['valor'],
                        "valor_sistema": item_s['valor'],
                        "status": status
                    })
                    break
            if not casado:
                registros.append({
                    "id": item_b['id'],
                    "descricao": item_b['descricao'],
                    "valor_banco":   item_b['valor'],
                    "valor_sistema": None,
                    "status": "APENAS_BANCO"
                })

        # Itens do sistema sem par
        for i, item_s in enumerate(itens_sistema):
            if not sistema_usados[i]:
                registros.append({
                    "id": item_s['id'],
                    "descricao": item_s['descricao'],
                    "valor_banco":   None,
                    "valor_sistema": item_s['valor'],
                    "status": "APENAS_SISTEMA"
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
        cliente = Groq(api_key=os.environ.get("GROQ_API_KEY",""))
        r = cliente.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=1000,
            messages=[{"role":"user","content":prompt}]
        )
        return r.choices[0].message.content
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
    try:
        u = usuario_logado()
        if not u:
            return jsonify({"erro":"Não autenticado."}), 401

        ids = ids_visiveis(u)
        if not ids:
            return jsonify([])

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
    except Exception as e:
        log.error("Erro listar_lotes: %s", e, exc_info=True)
        return jsonify({"erro": str(e)}), 500


@app.route("/api/lotes", methods=["POST"])
def criar_lote():
    try:
        u = usuario_logado()
        if not u:
            return jsonify({"erro":"Não autenticado."}), 401
        d    = request.get_json()
        nome = d.get("nome","").strip()
        if not nome:
            return jsonify({"erro":"Nome obrigatório."}), 400

        uid = u["dono_id"] if u["papel"] == "funcionario" else u["id"]
        agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.execute(
                "INSERT INTO lotes (nome, criado_em, usuario_id) VALUES (?,?,?)", (nome, agora, uid)
            )
            lote_id = cur.lastrowid
            conn.commit()
        return jsonify({"ok":True,"id":lote_id,"nome":nome})
    except Exception as e:
        log.error("Erro criar_lote: %s", e, exc_info=True)
        return jsonify({"ok":False,"erro": str(e)}), 500


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
