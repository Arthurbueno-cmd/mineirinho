import os
import time
import hashlib
import logging
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

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
console = Console()

PASTA_ENTRADA     = Path("./caixa_postal_banco")
PASTA_PROCESSADOS = Path("./processados")
PASTA_RELATORIOS  = Path("./relatorios_triade")
PASTA_ERROS       = Path("./erros")          # pasta nova: arquivos que falharam
DB_PATH           = Path("motor_seguro.db")

EXTENSOES_VALIDAS = {".csv", ".xlsx", ".xls"}

for pasta in [PASTA_ENTRADA, PASTA_PROCESSADOS, PASTA_RELATORIOS, PASTA_ERROS]:
    pasta.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# UTILITÁRIO: HASH DE SENHA
# ---------------------------------------------------------------------------

def _hash_senha(senha: str) -> str:
    """Retorna o SHA-256 da senha. Futuramente migrar para bcrypt."""
    return hashlib.sha256(senha.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# SISTEMA DE SEGURANÇA (SQLITE)
# ---------------------------------------------------------------------------

def configurar_seguranca() -> None:
    """Cria a tabela de usuários e insere o usuário padrão se não existir."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS usuarios "
            "(nome TEXT PRIMARY KEY, senha_hash TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT OR IGNORE INTO usuarios VALUES (?, ?)",
            ("arthur", _hash_senha("admin123")),
        )
        conn.commit()
    log.info("Segurança configurada.")


def validar_acesso(user: str, pw: str) -> bool:
    """Valida login comparando hash, nunca texto puro."""
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
    """Lê CSV ou Excel e retorna DataFrame. Lança ValueError para extensões não suportadas."""
    ext = caminho.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(caminho, dtype=str)
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(caminho, dtype=str)
    raise ValueError(f"Extensão não suportada: {ext}")


# ---------------------------------------------------------------------------
# FUNÇÃO DE MINERAÇÃO E MATCH
# ---------------------------------------------------------------------------

def minerar_e_conciliar(arquivo_nome: str) -> pd.DataFrame | None:
    """
    Lê o arquivo real, concilia com dados do banco e salva relatório.
    Retorna o DataFrame de resultados ou None em caso de erro.
    """
    caminho = PASTA_ENTRADA / arquivo_nome

    # --- Spinner visual (sem sleep artificial) ---
    with Progress(
        SpinnerColumn("earth"),
        TextColumn("[bold cyan]Mineirinho escavando {task.description}..."),
        transient=True,
    ) as progress:
        progress.add_task(description=arquivo_nome, total=None)

        try:
            dados_arquivo = _ler_arquivo(caminho)
        except Exception as exc:
            log.error("Falha ao ler '%s': %s", arquivo_nome, exc)
            console.print(f"[red]  Erro ao ler {arquivo_nome}: {exc}[/red]")
            caminho.rename(PASTA_ERROS / arquivo_nome)
            return None

        # Validação mínima de colunas esperadas
        colunas_esperadas = {"id", "valor"}
        if not colunas_esperadas.issubset(dados_arquivo.columns):
            msg = f"Colunas ausentes em '{arquivo_nome}': {colunas_esperadas - set(dados_arquivo.columns)}"
            log.error(msg)
            console.print(f"[red]  {msg}[/red]")
            caminho.rename(PASTA_ERROS / arquivo_nome)
            return None

        # Converte colunas numéricas
        dados_arquivo["valor"] = pd.to_numeric(dados_arquivo["valor"], errors="coerce")

        # --- Simulação de busca no banco de dados do sistema ---
        # TODO: substituir por query real ao banco de produção
        dados_banco = pd.DataFrame(
            {"id": dados_arquivo["id"].tolist(),
             "recebido": dados_arquivo["valor"].tolist()}  # placeholder
        )

        # Conciliação
        res = pd.merge(dados_arquivo, dados_banco, on="id", how="left")
        res["status"] = res.apply(
            lambda r: "OK" if r["valor"] == r["recebido"] else "DIVERGENTE", axis=1
        )

    # --- Salva relatório ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_log = f"log_triade_{timestamp}.csv"
    res.to_csv(PASTA_RELATORIOS / nome_log, index=False)
    log.info("Relatório salvo: %s", nome_log)

    # Move arquivo para processados
    caminho.rename(PASTA_PROCESSADOS / arquivo_nome)
    return res


# ---------------------------------------------------------------------------
# EXIBIÇÃO DOS RESULTADOS
# ---------------------------------------------------------------------------

def exibir_resultado(resultado: pd.DataFrame, nome_arquivo: str) -> None:
    table = Table(title=f"Resultados — {nome_arquivo}", border_style="blue")
    table.add_column("ID",     style="cyan",  no_wrap=True)
    table.add_column("Valor",  style="white", justify="right")
    table.add_column("Recebido", style="white", justify="right")
    table.add_column("Status", style="bold")

    for _, row in resultado.iterrows():
        cor = "green" if row["status"] == "OK" else "red"
        table.add_row(
            str(row["id"]),
            f"R$ {float(row['valor']):,.2f}",
            f"R$ {float(row['recebido']):,.2f}",
            f"[{cor}]{row['status']}[/{cor}]",
        )
    console.print(table)


# ---------------------------------------------------------------------------
# LOOP PRINCIPAL DO MOTOR
# ---------------------------------------------------------------------------

def iniciar_motor() -> None:
    configurar_seguranca()

    console.print(
        Panel.fit(
            "  [bold yellow]ASSISTENTE MINEIRINHO V3.0[/bold yellow]\n"
            "[white]Status: Offline[/white]",
            border_style="blue",
        )
    )

    user = console.input("[bold]Usuário:[/bold] ")
    pw   = console.input("[bold]Senha:[/bold]   ", password=True)

    if not validar_acesso(user, pw):
        console.print("[red]  Acesso negado.[/red]")
        log.warning("Tentativa de acesso negada para usuário '%s'.", user)
        return

    log.info("Usuário '%s' autenticado com sucesso.", user)
    console.print(
        Panel(
            f"  [bold green]MOTOR ONLINE E VIGIANDO![/bold green]\n"
            f"Olá, [cyan]{user}[/cyan]! Jogue seus arquivos em "
            f"[cyan]{PASTA_ENTRADA}[/cyan]\n"
            f"Formatos aceitos: {', '.join(EXTENSOES_VALIDAS)}",
            subtitle="Pressione CTRL+C para parar",
        )
    )

    try:
        while True:
            arquivos = [
                f for f in os.listdir(PASTA_ENTRADA)
                if (PASTA_ENTRADA / f).is_file()
                and Path(f).suffix.lower() in EXTENSOES_VALIDAS
            ]

            if arquivos:
                for arq in arquivos:
                    resultado = minerar_e_conciliar(arq)
                    if resultado is not None:
                        exibir_resultado(resultado, arq)
            else:
                # Pulsa um "." a cada ciclo para mostrar que está vivo
                console.print("[dim]· aguardando arquivos...[/dim]", end="\r")

            time.sleep(2)

    except KeyboardInterrupt:
        console.print(
            f"\n[blue]Encerrando o Motor... Até logo, {user}![/blue]"
        )
        log.info("Motor encerrado pelo usuário '%s'.", user)


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    iniciar_motor()
