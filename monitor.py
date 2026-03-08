"""
BCB Pix Normativas Monitor - v9
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TWILIO_ACCOUNT_SID   = os.environ["TWILIO_ACCOUNT_SID"].strip()
TWILIO_AUTH_TOKEN    = os.environ["TWILIO_AUTH_TOKEN"].strip()
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886").strip()
WHATSAPP_TO          = os.environ["WHATSAPP_TO"].strip()
ANTHROPIC_API_KEY    = os.environ["ANTHROPIC_API_KEY"].strip()
FORCE_RESET          = os.environ.get("FORCE_RESET", "false").lower() == "true"

print(f"[DIAG] WHATSAPP_TO='{WHATSAPP_TO}' len={len(WHATSAPP_TO)}", flush=True)

STATE_FILE = Path("state.json")
BCB_NORMATIVO_BASE = "https://www.bcb.gov.br/estabilidadefinanceira/exibenormativo"
BCB_BUSCA_URL = "https://www.bcb.gov.br/estabilidadefinanceira/normativos?tipo=Resolucao+BCB&assunto=Pix&formato=Lista&pagina=1"

# Tenta multiplos User-Agents para burlar bloqueio do BCB
HEADERS_LIST = [
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "pt-BR,pt;q=0.9",
    },
]

NORMATIVOS_CONHECIDOS = [
    {
        "id": "ResolucaoBCB-407",
        "tipo": "Resolucao BCB",
        "numero": "407",
        "titulo": "Altera o Regulamento Pix para implementar o Pix Automatico",
        "data_publicacao": "2024-08-02",
        "url": BCB_NORMATIVO_BASE + "?tipo=Resolu%C3%A7%C3%A3o+BCB&numero=407",
    },
]


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"seen_ids": [], "last_check": None}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


async def fetch_latest_normativos():
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(BCB_BUSCA_URL, headers=HEADERS_LIST[0])
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
                numero_match = re.search(r"n[o]?\s*(\d+)", texto, re.IGNORECASE)
            if not numero_match:
                continue
            numero = numero_match.group(1)
            data_cell = row.find("td", class_=re.compile("data|date", re.I))
            data = data_cell.get_text(strip=True) if data_cell else ""
            titulo = texto[:200] if texto else "Resolucao BCB n " + numero
            url = href if href.startswith("http") else "https://www.bcb.gov.br" + href
            normativos.append({
                "id": "ResolucaoBCB-" + numero,
                "tipo": "Resolucao BCB",
                "numero": numero,
                "titulo": titulo,
                "data_publicacao": data,
                "url": url,
            })
        except Exception as e:
            log.debug("Erro parseando linha: " + str(e))
    if not normativos:
        log.info("Scraping HTML sem resultados, usando varredura sequencial")
        normativos = await check_sequential_normativos()
    return normativos


async def check_sequential_normativos():
    state = load_state()
    seen_ids = state.get("seen_ids", [])
    numeros_vistos = [int(re.search(r"(\d+)$", sid).group(1)) for sid in seen_ids if re.search(r"(\d+)$", sid)]
    ultimo_numero = max(numeros_vistos) if numeros_vistos else 444

    normativos = []
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for num in range(ultimo_numero + 1, ultimo_numero + 6):
            url = BCB_NORMATIVO_BASE + "?tipo=Resolu%C3%A7%C3%A3o+BCB&numero=" + str(num)
            try:
                resp = await client.head(url, headers=HEADERS_LIST[0])
                if resp.status_code == 200:
                    resp_get = await client.get(url, headers=HEADERS_LIST[0])
                    titulo = "Resolucao BCB n " + str(num)
                    soup = BeautifulSoup(resp_get.text, "html.parser")
                    h1 = soup.find("h1") or soup.find("h2")
                    if h1:
                        titulo = h1.get_text(strip=True)
                    normativos.append({
                        "id": "ResolucaoBCB-" + str(num),
                        "tipo": "Resolucao BCB",
                        "numero": str(num),
                        "titulo": titulo,
                        "data_publicacao": datetime.now().strftime("%Y-%m-%d"),
                        "url": url,
                    })
                    log.info("Nova Resolucao BCB encontrada: n " + str(num))
            except Exception as e:
                log.debug("Erro verificando n " + str(num) + ": " + str(e))
    return normativos


async def fetch_normativo_texto(url):
    """Tenta extrair o texto completo com diferentes headers."""
    for headers in HEADERS_LIST:
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                resp = await client.get(url, headers=headers)
                resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
                tag.decompose()
            # Tenta pegar o conteudo principal
            main = soup.find("main") or soup.find("article") or soup.find(id=re.compile("conteudo|content|main", re.I))
            texto = main.get_text(separator="\n", strip=True) if main else soup.get_text(separator="\n", strip=True)
            texto = re.sub(r"\n{3,}", "\n\n", texto)
            if len(texto) > 300:
                log.info(f"Texto extraido com sucesso: {len(texto)} chars")
                return texto[:4000]
        except Exception as e:
            log.warning("Tentativa de extracao falhou: " + str(e))
            continue
    log.warning("Nao foi possivel extrair texto completo da norma")
    return ""


def gerar_mensagem(normativo, texto_completo):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    tem_texto = len(texto_completo) > 300
    if tem_texto:
        contexto = "Use o texto abaixo para embasar o resumo com informacoes especificas e reais:\n\n" + texto_completo
    else:
        contexto = (
            "O texto completo nao estava disponivel. Use seu conhecimento especializado sobre "
            "regulacao do BCB e o titulo da resolucao para inferir os impactos com o maximo de "
            "especificidade possivel. Mencione artigos, limites, prazos ou tecnicalidades que "
            "tipicamente aparecem em normas desse tipo."
        )

    prompt = (
        "Voce e especialista em regulacao do sistema de pagamentos brasileiro, com foco no Pix.\n\n"
        "Normativo: Resolucao BCB n " + normativo["numero"] + "\n"
        "Data: " + normativo["data_publicacao"] + "\n"
        "Titulo: " + normativo["titulo"] + "\n\n"
        + contexto + "\n\n"
        "Gere uma mensagem de WhatsApp com exatamente este formato, sendo especifico e tecnico:\n\n"
        "*Resolucao BCB " + normativo["numero"] + "* (" + normativo["data_publicacao"] + ")\n"
        "_[frase direta explicando o que a norma faz]_\n\n"
        "*O que diz:*\n"
        "- [ponto especifico da norma, com numeros/limites se houver]\n"
        "- [segundo ponto especifico]\n\n"
        "*Impacto:*\n"
        "- [impacto concreto no negocio da instituicao]\n"
        "- [impacto concreto no cliente final]\n\n"
        "*Acoes necessarias:*\n"
        "- [acao especifica 1 — ex: adaptar fluxo X, implementar campo Y]\n"
        "- [acao especifica 2]\n"
        "- [acao especifica 3 se houver]\n\n"
        "*Prazo: [data concreta ou A confirmar]*\n\n"
        "Detalhes: " + normativo["url"] + "\n\n"
        "Regras: maximo 200 palavras. Sem genericos como 'adaptar processos' ou 'revisar politicas'. "
        "Seja especifico sobre o que muda no produto, na jornada do usuario ou na integracao tecnica."
    )

    message = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text[:1400]


def enviar_whatsapp(mensagem):
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    msg = client.messages.create(
        from_=TWILIO_WHATSAPP_FROM,
        to=WHATSAPP_TO,
        body=mensagem
    )
    log.info("WhatsApp enviado SID=" + msg.sid)
    return msg.sid


async def run():
    log.info("=== BCB Pix Monitor iniciando ===")

    if FORCE_RESET:
        log.info("FORCE_RESET ativado - modo de teste")
        seen_ids = []
        normativos = NORMATIVOS_CONHECIDOS[:1]
    else:
        state = load_state()
        seen_ids = state.get("seen_ids", [])
        normativos = await fetch_latest_normativos()

    log.info("Encontrados " + str(len(normativos)) + " normativos")
    novos = [n for n in normativos if n["id"] not in seen_ids]
    log.info(str(len(novos)) + " novos normativos a processar")

    for normativo in reversed(novos):
        log.info("Processando: " + normativo["id"])
        try:
            texto = await fetch_normativo_texto(normativo["url"])
            mensagem = gerar_mensagem(normativo, texto)
            enviar_whatsapp(mensagem)
            seen_ids.append(normativo["id"])
            log.info("Notificacao enviada: " + normativo["id"])
            await asyncio.sleep(3)
        except Exception as e:
            log.error("Erro processando " + normativo["id"] + ": " + str(e), exc_info=True)

    if not FORCE_RESET:
        state = load_state()
        state["seen_ids"] = seen_ids[-500:]
        state["last_check"] = datetime.now(timezone.utc).isoformat()
        save_state(state)

    log.info("=== Monitor finalizado ===")


if __name__ == "__main__":
    asyncio.run(run())
