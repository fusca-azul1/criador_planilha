import logging
import re
import pandas as pd
import os
import asyncio
from openpyxl import load_workbook
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, CommandHandler, filters

# Configuração de Logs
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Controle de lote e estado
batch_control = {"timer_task": None, "repetidos_count": 0, "processados_count": 0}
NOME_PLANILHA = "Base_Dados.xlsx"

CAMPOS_PARA_BUSCAR = {
    "CPF": r"CPF:\s*([\d.-]+)",
    "Nome": r"NOME:\s*(.*)",
    "Nascimento": r"NASCIMENTO:\s*([\d/]+)",
    "Mãe": r"MÃE:\s*(.*)",
    "RG": r"RG:\s*([\d.-]+)",
    "Renda": r"RENDA:\s*([\d,.]+)",
    "Score CSB8": r"CSB8:\s*(\d+)",
    "Email": r"EMAIL:\s*([\w.-]+@[\w.-]+)"
}

def ajustar_largura_colunas(caminho):
    wb = load_workbook(caminho)
    ws = wb.active
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length: max_length = len(str(cell.value))
            except: pass
        ws.column_dimensions[column].width = max_length + 3
    wb.save(caminho)

def extrair_dados(texto):
    dados = {}
    for coluna, regex in CAMPOS_PARA_BUSCAR.items():
        match = re.search(regex, texto, re.IGNORECASE)
        if match:
            valor = match.group(1).strip()
            dados[coluna] = valor if valor.upper() != "N/A" else "Sem informação"
        else:
            dados[coluna] = "Sem informação"
    telefones = re.findall(r"\(\d{2}\)\s*[\d\s-]{8,11}", texto)
    dados["Telefones"] = ", ".join(telefones) if telefones else "Sem informação"
    return dados

# --- COMANDOS ---

async def comando_iniciar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Responde com o comando para iniciar no CMD"""
    msg = await update.message.reply_text("💻 Para iniciar o bot no seu computador, use o comando:\n\n`python bot_extrator.py` (dentro da pasta do projeto)", parse_mode='Markdown')
    # Apaga a instrução após 1 minuto para não poluir
    asyncio.create_task(deletar_mensagem_depois(msg, 60))

async def comando_planilha(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Envia a planilha e apaga após 1 minuto"""
    if os.path.exists(NOME_PLANILHA):
        doc = await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(NOME_PLANILHA, 'rb'),
            caption="📊 Aqui está a planilha atualizada.\n(Esta mensagem será apagada em 1 minuto)"
        )
        # Agenda a exclusão do arquivo enviado
        asyncio.create_task(deletar_mensagem_depois(doc, 60))
    else:
        msg = await update.message.reply_text("❌ A planilha ainda não foi criada. Envie arquivos .txt primeiro!")
        asyncio.create_task(deletar_mensagem_depois(msg, 10))

# --- PROCESSAMENTO ---

async def finalizar_servico(chat_id, context):
    await asyncio.sleep(5)
    texto = f"✅ Serviço completo\n({batch_control['repetidos_count']} arquivos repetidos ignorados)"
    await context.bot.send_message(chat_id=chat_id, text=texto)
    batch_control.update({"repetidos_count": 0, "processados_count": 0, "timer_task": None})

async def processar_documento(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg.document or not msg.document.file_name.endswith('.txt'): return 

    if batch_control['timer_task']: batch_control['timer_task'].cancel()
    
    temp = f"temp_{msg.document.file_unique_id}.txt"
    file = await msg.document.get_file()
    await file.download_to_drive(temp)
    
    try:
        try:
            with open(temp, 'r', encoding='utf-8') as f: conteudo = f.read()
        except:
            with open(temp, 'r', encoding='latin-1') as f: conteudo = f.read()
        
        dados = extrair_dados(conteudo)
        cpf = str(dados['CPF'])
        repetido = False

        if os.path.exists(NOME_PLANILHA):
            df_old = pd.read_excel(NOME_PLANILHA)
            if cpf in df_old['CPF'].astype(str).values: repetido = True

        if repetido:
            batch_control['repetidos_count'] += 1
        else:
            df = pd.concat([pd.read_excel(NOME_PLANILHA), pd.DataFrame([dados])], ignore_index=True) if os.path.exists(NOME_PLANILHA) else pd.DataFrame([dados])
            df.to_excel(NOME_PLANILHA, index=False)
            ajustar_largura_colunas(NOME_PLANILHA)
            batch_control['processados_count'] += 1

        try: await msg.delete() # Apaga o .txt do usuário
        except: pass

        if not repetido:
            aviso = await msg.reply_text(f"📥 Processando: {dados['Nome']}...")
            asyncio.create_task(deletar_mensagem_depois(aviso, 5))

    except Exception as e: logging.error(f"Erro: {e}")
    finally:
        if os.path.exists(temp): os.remove(temp)
        batch_control['timer_task'] = asyncio.create_task(finalizar_servico(msg.chat_id, context))

async def deletar_mensagem_depois(mensagem, tempo):
    await asyncio.sleep(tempo)
    try: await mensagem.delete()
    except: pass

if __name__ == '__main__':
    TOKEN = '8682744264:AAEuRc_qeeILf7jXAjsAIn-UpMe0DQ2Dt3Q'
    app = ApplicationBuilder().token(TOKEN).build()
    
    # Handlers de Comandos
    app.add_handler(CommandHandler("iniciar", comando_iniciar))
    app.add_handler(CommandHandler("planilha", comando_planilha))
    
    # Handler de Documentos
    app.add_handler(MessageHandler(filters.Document.ALL, processar_documento))
    
    print("Bot Finalizado com Sucesso! Comandos /iniciar e /planilha ativos.")
    app.run_polling()