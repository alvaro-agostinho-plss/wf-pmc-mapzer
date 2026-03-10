#!/usr/bin/env python3
"""
Envia e-mail de teste para EMAIL_RELTORIO_TOTAL.
Uso: python scripts/test_envio_email.py
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.config import SMTPConfig
from src.reports import enviar_email

def main():
    config = SMTPConfig()
    destino = config.email_relatorio_total or "alvaro.agostinho@plss.com.br"
    html = """
    <html><body style="font-family: Arial; padding: 20px;">
        <h2>Teste de envio - Mapzer</h2>
        <p>Este é um e-mail de teste do sistema de relatórios Mapzer.</p>
        <p>Se você recebeu esta mensagem, o SMTP está configurado corretamente.</p>
        <hr><small>Enviado automaticamente.</small>
    </body></html>
    """
    enviar_email(
        [destino.strip()],
        "Teste Mapzer - Configuração SMTP",
        html,
        config,
    )
    print(f"✓ E-mail de teste enviado para {destino}")

if __name__ == "__main__":
    main()
