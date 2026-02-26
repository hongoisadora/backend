"""
BCB Pix Normativas Monitor v2
===============================
Monitora resoluções do Banco Central relacionadas ao Pix
e envia alertas via WhatsApp usando Twilio + resumo via Claude AI.

Estratégia: faz scraping da página de busca do BCB filtrada por
"Resolução BCB" + "Pix", detecta novos números e notifica.
"""

import os
import json
import logging
import httpx
import asyncio
import re
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup
from twilio.rest import Client as TwilioClient
import anthropic

# ── Configuração ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

TWILIO_ACCOUNT_SID   = os.environ["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN    = os.environ["TWILIO_AUTH_TOKEN"]
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
WHATSAPP_TO          = os.environ["WHATSAPP_TO"]
ANTHROPIC_API_KEY    = os.environ["ANTHROPIC_API_KEY"]

STATE_FILE = Path("state.json")

# URL de busca no portal BCB — filtra por Resolução BCB com texto "Pix"
BCB_BUSCA_URL = (
    "https://www.bcb.gov.br/estabilidadefinanceira/normativos"
    "?tipo=Resolucao+BCB&assunto=Pix&formato=Lista&pagina=1"
)
BCB_NORMATIVO_BASE = "https://www.bcb.gov.br/estabilidadefinanceira/exibenormativo"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PixMonitor/2.0)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "pt-BR,pt;q=0.9",
}


# ── Estado persistente ────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"seen_ids": [], "last_check": None}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


# ── Scraping do portal BCB ────────────────────────────────────────────────────
async def fetch_latest_normativos() -> list[dict]:
    """
    Faz scraping da página de normativos do BCB filtrando por Pix.
    Retorna lista de dicts com dados de cada normativo encontrado.
    """
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(BCB_BUSCA_URL, headers=HEADERS)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    normativos = []

    # O portal BCB lista normativos em tabelas ou cards — tentamos ambos
    # Padrão 1: tabela com links para normativos
    rows = soup.select("table tbody tr") or soup.select(".normativo-item")

    for row in rows[:20]:  # Pega os 20 mais recentes
        try:
            link = row.find("a", href=True)
            if not link:
                continue

            href = link.get("href", "")
            texto = link.get_text(strip=True)

            # Extrai número da resolução do href ou texto
            numero_match = re.search(r"numero[=\-/](\d+)", href, re.IGNORECASE)
            if not numero_match:
                numero_match = re.search(r"n[ºo°]?\s*(\d+)", texto, re.IGNORECASE)
            if not numero_match:
                continue

            numero = numero_match.group(1)

            # Data de publicação
            data_cell = row.find("td", class_=re.compile("data|date", re.I))
            data = data_cell.get_text(strip=True) if data_cell else ""

            # Título/ementa
            titulo = texto[:200] if texto else f"Resolução BCB nº {numero}"

            url = href if href.startswith("http") else f"https://www.bcb.gov.br{href}"

            normativos.append({
                "id": f"ResolucaoBCB-{numero}",
                "tipo": "Resolução BCB",
                "numero": numero,
                "titulo": titulo,
                "data_publicacao": data,
                "url": url,
                "ementa": titulo,
            })
        except Exception as e:
            log.debug(f"Erro parseando linha: {e}")
            continue

    # Se o scraping não retornou resultados, usa estratégia alternativa:
    # varre números sequencialmente a partir do último visto
    if not normativos:
        log.info("Scraping HTML falhou, usando estratégia de varredura sequencial")
        normativos = await check_sequential_normativos()

    return normativos


async def check_sequential_normativos() -> list[dict]:
    """
    Estratégia alternativa: verifica se existem Resoluções BCB com números
    maiores que o último registrado, fazendo HEAD requests para confirmar.
    """
    state = load_state()
    seen_ids = state.get("seen_ids", [])

    # Descobre o maior número já visto
    numeros_vistos = []
    for sid in seen_ids:
        m = re.search(r"(\d+)$", sid)
        if m:
            numeros_vistos.append(int(m.group(1)))

    ultimo_numero = max(numeros_vistos) if numeros_vistos else 429  # última conhecida

    normativos = []
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        # Verifica os próximos 5 números após o último visto
        for num in range(ultimo_numero + 1, ultimo_numero + 6):
            url = f"{BCB_NORMATIVO_BASE}?tipo=Resolu%C3%A7%C3%A3o+BCB&numero={num}"
            try:
                resp = await client.head(url, headers=HEADERS)
                if resp.status_code == 200:
                    # Existe! Busca o título fazendo GET
                    resp_get = await client.get(url, headers=HEADERS)
                    titulo = f"Resolução BCB nº {num}"
                    soup = BeautifulSoup(resp_get.text, "html.parser")
                    h1 = soup.find("h1") or soup.find("h2")
                    if h1:
                        titulo = h1.get_text(strip=True)

                    normativos.append({
                        "id": f"ResolucaoBCB-{num}",
                        "tipo": "Resolução BCB",
                        "numero": str(num),
                        "titulo": titulo,
                        "data_publicacao": datetime.now().strftime("%Y-%m-%d"),
                        "url": url,
                        "ementa": titulo,
                    })
                    log.info(f"✅ Nova Resolução BCB encontrada: nº {num}")
            except Exception as e:
                log.debug(f"Erro verificando nº {num}: {e}")

    return normativos


async def fetch_normativo_texto(url: str) -> str:
    """Extrai o texto completo do normativo para o resumo da IA."""
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url, headers=HEADERS)
            resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        return text[:6000]
    except Exception as e:
        log.warning(f"Não foi possível extrair texto completo: {e}")
        return ""


# ── Resumo com Claude ─────────────────────────────────────────────────────────
def gerar_resumo(normativo: dict, texto_completo: str) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    contexto = (
        f"\n\nTexto da norma:\n{texto_completo}"
        if texto_completo
        else f"\n\nEmenta: {normativo['ementa']}"
    )

    prompt = f"""Você é especialista em regulação do sistema de pagamentos brasileiro, 
com foco no arranjo Pix. Analise a normativa abaixo e produza um resumo executivo 
para o time de produto de uma instituição participante do Pix.

Normativo: {normativo['tipo']} nº {normativo['numero']}
Título: {normativo['titulo']}
Data de publicação: {normativo['data_publicacao']}
{contexto}

Produza um resumo com EXATAMENTE este formato (máx. 350 palavras):

🎯 *O que mudou*: [1-2 frases diretas sobre o que a norma altera]

📌 *Impacto para o produto*: [bullet points com impactos concretos para o time de produto Pix]

⏰ *Prazo de adequação*: [datas e prazos mencionados, ou "Não especificado"]

🔗 *Para ler a norma completa*: {normativo['url']}

Seja objetivo, técnico e direto."""

    message = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=700,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text


# ── Envio WhatsApp ─────────────────────────────────────────────────────────────
def enviar_whatsapp(mensagem: str):
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    msg = client.messages.create(
        from_=TWILIO_WHATSAPP_FROM,
        to=WHATSAPP_TO,
        body=mensagem
    )
    log.info(f"WhatsApp enviado: SID={msg.sid}")
    return msg.sid


def montar_mensagem(normativo: dict, resumo: str) -> str:
    return f"""🏦 *Nova Normativa BCB — Pix*

📄 *{normativo['tipo']} nº {normativo['numero']}*
_{normativo['titulo'][:100]}_
📅 Publicada em: {normativo['data_publicacao']}

{resumo}

---
_PixMonitor · Atualização automática_"""


# ── Fluxo principal ───────────────────────────────────────────────────────────
async def run():
    log.info("=== BCB Pix Monitor v2 iniciando ===")
    state = load_state()
    seen_ids: list = state.get("seen_ids", [])

    normativos = await fetch_latest_normativos()
    log.info(f"Encontrados {len(normativos)} normativos na consulta")

    novos = [n for n in normativos if n["id"] not in seen_ids]
    log.info(f"{len(novos)} novos normativos a processar")

    for normativo in reversed(novos):
        log.info(f"Processando: {normativo['id']}")
        try:
            texto = await fetch_normativo_texto(normativo["url"])
            resumo = gerar_resumo(normativo, texto)
            mensagem = montar_mensagem(normativo, resumo)
            enviar_whatsapp(mensagem)
            seen_ids.append(normativo["id"])
            log.info(f"✅ Notificação enviada: {normativo['id']}")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"Erro processando {normativo['id']}: {e}", exc_info=True)

    state["seen_ids"] = seen_ids[-500:]
    state["last_check"] = datetime.now(timezone.utc).isoformat()
    save_state(state)
    log.info("=== Monitor v2 finalizado ===")


if __name__ == "__main__":
    asyncio.run(run())
