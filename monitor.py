"""
BCB Pix Normativas Monitor
===========================
Monitora resoluções do Banco Central relacionadas ao Pix
e envia alertas via WhatsApp usando Twilio + resumo via Claude AI.

Deploy: Railway / Render / Fly.io (qualquer plataforma com cron jobs)
"""

import os
import json
import hashlib
import logging
import httpx
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup
from twilio.rest import Client as TwilioClient
import anthropic

# ── Configuração ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Variáveis de ambiente (configure no .env ou no painel do seu host)
TWILIO_ACCOUNT_SID   = os.environ["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN    = os.environ["TWILIO_AUTH_TOKEN"]
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")  # Twilio Sandbox
WHATSAPP_TO          = os.environ["WHATSAPP_TO"]          # Ex: whatsapp:+5511999999999
ANTHROPIC_API_KEY    = os.environ["ANTHROPIC_API_KEY"]

# Arquivo local para rastrear o que já foi notificado
STATE_FILE = Path("state.json")

# URL da pesquisa de normativos Pix no portal BCB
BCB_SEARCH_URL = (
    "https://www.bcb.gov.br/api/normativo/pesquisar"
    "?assunto=Pix&tipo=Resolucao+BCB&pagina=1&quantidade=10"
    "&ordem=dataPublicacao%20desc"
)

BCB_NORMATIVO_BASE = "https://www.bcb.gov.br/estabilidadefinanceira/exibenormativo"


# ── Estado persistente ────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"seen_ids": [], "last_check": None}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


# ── Busca de normativos no BCB ────────────────────────────────────────────────
async def fetch_latest_normativos() -> list[dict]:
    """
    Consulta a API pública do BCB e retorna lista de normativos Pix.
    Cada item contém: id, tipo, numero, titulo, dataPublicacao, url
    """
    headers = {
        "User-Agent": "PixMonitor/1.0 (PM Bot; contato@suainstituicao.com.br)",
        "Accept": "application/json",
    }
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        resp = await client.get(BCB_SEARCH_URL, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    normativos = []
    for item in data.get("conteudo", []):
        numero = item.get("numero", "")
        tipo   = item.get("tipo", "Resolução BCB")
        url    = f"{BCB_NORMATIVO_BASE}?tipo={tipo}&numero={numero}"
        normativos.append({
            "id":              f"{tipo}-{numero}",
            "tipo":            tipo,
            "numero":          numero,
            "titulo":          item.get("titulo", ""),
            "data_publicacao": item.get("dataPublicacao", "")[:10],
            "url":             url,
            "ementa":          item.get("ementa", ""),
        })
    return normativos


async def fetch_normativo_texto(url: str) -> str:
    """Tenta extrair o texto completo do normativo para o resumo."""
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # Remove scripts e estilos
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        # Limita para não explodir o contexto da IA
        return text[:6000]
    except Exception as e:
        log.warning(f"Não foi possível extrair texto completo: {e}")
        return ""


# ── Resumo inteligente com Claude ─────────────────────────────────────────────
def gerar_resumo(normativo: dict, texto_completo: str) -> str:
    """
    Usa Claude para gerar um resumo executivo focado em impacto para o Pix.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    contexto_texto = (
        f"\n\nTexto completo da norma:\n{texto_completo}"
        if texto_completo
        else f"\n\nEmenta: {normativo['ementa']}"
    )

    prompt = f"""Você é especialista em regulação do sistema de pagamentos brasileiro, 
com foco no arranjo Pix. Analise a normativa abaixo e produza um resumo executivo 
para o time de produto de uma instituição participante do Pix.

Normativo: {normativo['tipo']} nº {normativo['numero']}
Título: {normativo['titulo']}
Data de publicação: {normativo['data_publicacao']}
{contexto_texto}

Produza um resumo com EXATAMENTE este formato (máx. 350 palavras no total):

🎯 *O que mudou*: [1-2 frases diretas sobre o que a norma altera]

📌 *Impacto para o produto*: [bullet points com impactos concretos para o time de produto Pix]

⏰ *Prazo de adequação*: [datas e prazos mencionados, ou "Não especificado"]

🔗 *Para ler a norma completa*: {normativo['url']}

Seja objetivo, técnico e direto. Foque no que o time de produto PRECISA saber e fazer."""

    message = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=700,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text


# ── Envio WhatsApp via Twilio ─────────────────────────────────────────────────
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
_{normativo['titulo']}_
📅 Publicada em: {normativo['data_publicacao']}

{resumo}

---
_PixMonitor · Atualização automática_"""


# ── Fluxo principal ───────────────────────────────────────────────────────────
async def run():
    log.info("=== BCB Pix Monitor iniciando ===")
    state = load_state()
    seen_ids: list = state.get("seen_ids", [])

    normativos = await fetch_latest_normativos()
    log.info(f"Encontrados {len(normativos)} normativos na consulta")

    novos = [n for n in normativos if n["id"] not in seen_ids]
    log.info(f"{len(novos)} novos normativos a processar")

    for normativo in reversed(novos):  # Mais antigo primeiro
        log.info(f"Processando: {normativo['id']}")
        try:
            texto = await fetch_normativo_texto(normativo["url"])
            resumo = gerar_resumo(normativo, texto)
            mensagem = montar_mensagem(normativo, resumo)
            enviar_whatsapp(mensagem)
            seen_ids.append(normativo["id"])
            log.info(f"✅ Notificação enviada para {normativo['id']}")
            # Pequena pausa para não sobrecarregar APIs
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"Erro processando {normativo['id']}: {e}", exc_info=True)

    state["seen_ids"] = seen_ids[-500:]  # Mantém histórico dos últimos 500
    state["last_check"] = datetime.now(timezone.utc).isoformat()
    save_state(state)
    log.info("=== Monitor finalizado ===")


if __name__ == "__main__":
    asyncio.run(run())
