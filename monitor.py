"""
BCB Pix Normativas Monitor v3
===============================
Monitora resoluções do Banco Central relacionadas ao Pix
e envia alertas via WhatsApp usando Twilio + resumo via Claude AI.
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

TWILIO_ACCOUNT_SID   = os.environ["TWILIO_ACCOUNT_SID"].strip()
TWILIO_AUTH_TOKEN    = os.environ["TWILIO_AUTH_TOKEN"].strip()
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
WHATSAPP_TO          = os.environ["WHATSAPP_TO"].strip()
ANTHROPIC_API_KEY    = os.environ["ANTHROPIC_API_KEY"].strip()

# Se FORCE_RESET=true, ignora o histórico e reprocessa tudo (modo de teste)
FORCE_RESET = os.environ.get("FORCE_RESET", "false").lower() == "true"

# Diagnóstico — mostra exatamente o que está sendo usado
import sys
print(f"[DIAG] WHATSAPP_TO='{WHATSAPP_TO}' len={len(WHATSAPP_TO)}", flush=True)
print(f"[DIAG] FROM='{TWILIO_WHATSAPP_FROM}'", flush=True)

STATE_FILE = Path("state.json")

BCB_BUSCA_URL = (
    "https://www.bcb.gov.br/estabilidadefinanceira/normativos"
    "?tipo=Resolucao+BCB&assunto=Pix&formato=Lista&pagina=1"
)
BCB_NORMATIVO_BASE = "https://www.bcb.gov.br/estabilidadefinanceira/exibenormativo"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PixMonitor/3.0)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

# Resoluções BCB conhecidas sobre Pix — usadas como fallback no FORCE_RESET
NORMATIVOS_CONHECIDOS = [
    {
        "id": "ResolucaoBCB-407",
        "tipo": "Resolução BCB",
        "numero": "407",
        "titulo": "Altera o Regulamento Pix para implementar o Pix Automático",
        "data_publicacao": "2024-08-02",
        "url": f"{BCB_NORMATIVO_BASE}?tipo=Resolu%C3%A7%C3%A3o+BCB&numero=407",
        "ementa": "Implementa o Pix Automático como modalidade de débito recorrente.",
    },
    {
        "id": "ResolucaoBCB-403",
        "tipo": "Resolução BCB",
        "numero": "403",
        "titulo": "Aprimora mecanismos de segurança e gerenciamento de risco de fraude no Pix",
        "data_publicacao": "2024-07-22",
        "url": f"{BCB_NORMATIVO_BASE}?tipo=Resolu%C3%A7%C3%A3o+BCB&numero=403",
        "ementa": "Exige solução antifraude com detecção de transações atípicas.",
    },
]


# ── Estado persistente ────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"seen_ids": [], "last_check": None}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


# ── Scraping do portal BCB ────────────────────────────────────────────────────
async def fetch_latest_normativos() -> list[dict]:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(BCB_BUSCA_URL, headers=HEADERS)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    normativos = []
    rows = soup.select("table tbody tr") or soup.select(".normativo-item")

    for row in rows[:20]:
        try:
            link = row.find("a", href=True)
            if not link:
                continue
            href = link.get("href", "")
            texto = link.get_text(strip=True)

            numero_match = re.search(r"numero[=\-/](\d+)", href, re.IGNORECASE)
            if not numero_match:
                numero_match = re.search(r"n[ºo°]?\s*(\d+)", texto, re.IGNORECASE)
            if not numero_match:
                continue

            numero = numero_match.group(1)
            data_cell = row.find("td", class_=re.compile("data|date", re.I))
            data = data_cell.get_text(strip=True) if data_cell else ""
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

    if not normativos:
        log.info("Scraping HTML sem resultados, usando varredura sequencial")
        normativos = await check_sequential_normativos()

    return normativos


async def check_sequential_normativos() -> list[dict]:
    state = load_state()
    seen_ids = state.get("seen_ids", [])

    numeros_vistos = []
    for sid in seen_ids:
        m = re.search(r"(\d+)$", sid)
        if m:
            numeros_vistos.append(int(m.group(1)))

    ultimo_numero = max(numeros_vistos) if numeros_vistos else 429

    normativos = []
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for num in range(ultimo_numero + 1, ultimo_numero + 6):
            url = f"{BCB_NORMATIVO_BASE}?tipo=Resolu%C3%A7%C3%A3o+BCB&numero={num}"
            try:
                resp = await client.head(url, headers=HEADERS)
                if resp.status_code == 200:
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
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url, headers=HEADERS)
            resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)[:6000]
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
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text


# ── Envio WhatsApp ─────────────────────────────────────────────────────────────
def enviar_whatsapp(mensagem: str):
    mensagem = mensagem[:1500]  # Twilio limite de 1600 chars
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

"""


# ── Fluxo principal ───────────────────────────────────────────────────────────
async def run():
    log.info("=== BCB Pix Monitor v3 iniciando ===")

    if FORCE_RESET:
        log.info("⚠️  FORCE_RESET ativado — modo de teste, ignorando histórico")
        seen_ids = []
        # No modo de teste usa normativos conhecidos para garantir envio
        normativos = NORMATIVOS_CONHECIDOS[:1]  # Envia só 1 para não spammar
    else:
        state = load_state()
        seen_ids = state.get("seen_ids", [])
        normativos = await fetch_latest_normativos()

    log.info(f"Encontrados {len(normativos)} normativos")

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

    if not FORCE_RESET:
        state = load_state()
        state["seen_ids"] = seen_ids[-500:]
        state["last_check"] = datetime.now(timezone.utc).isoformat()
        save_state(state)

    log.info("=== Monitor v3 finalizado ===")


if __name__ == "__main__":
    asyncio.run(run())
