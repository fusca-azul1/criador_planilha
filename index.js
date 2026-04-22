const { Telegraf } = require('telegraf');
const fs = require('fs-extra');
const XLSX = require('xlsx');
const fetch = require('node-fetch');

require('dotenv').config();
const bot = new Telegraf(process.env.BOT_TOKEN);

const NOME_PLANILHA = 'Base_Dados.xlsx';

let batch_control = {
    timer: null,
    repetidos: 0,
    processados: 0,
    chatId: null
};

const CAMPOS = {
    CPF: /CPF:\s*([\d.-]+)/i,
    Nome: /NOME:\s*(.*)/i,
    Nascimento: /NASCIMENTO:\s*([\d/]+)/i,
    "Mãe": /MÃE:\s*(.*)/i,
    RG: /RG:\s*([\d.-]+)/i,
    Renda: /RENDA:\s*([\d,.]+)/i,
    "Score CSB8": /CSB8:\s*(\d+)/i,
    Email: /EMAIL:\s*([\w.-]+@[\w.-]+)/i
};

// 🔍 Extrair dados
function extrairDados(texto) {
    let dados = {};

    for (let campo in CAMPOS) {
        let match = texto.match(CAMPOS[campo]);
        dados[campo] = match ? match[1].trim() : "Sem informação";
    }

    let telefones = texto.match(/\(\d{2}\)\s*[\d\s-]{8,11}/g);
    dados["Telefones"] = telefones ? telefones.join(", ") : "Sem informação";

    return dados;
}

// 📊 Salvar Excel
function salvarExcel(novoDado) {
    let dados = [];

    if (fs.existsSync(NOME_PLANILHA)) {
        let wb = XLSX.readFile(NOME_PLANILHA);
        let ws = wb.Sheets[wb.SheetNames[0]];
        dados = XLSX.utils.sheet_to_json(ws);
    }

    if (dados.some(d => String(d.CPF) === String(novoDado.CPF))) {
        return false;
    }

    dados.push(novoDado);

    let ws = XLSX.utils.json_to_sheet(dados);
    let wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Dados');

    XLSX.writeFile(wb, NOME_PLANILHA);
    return true;
}

// 🚫 Anti flood (retry automático)
async function enviarComRetry(ctx, texto) {
    try {
        await ctx.telegram.sendMessage(batch_control.chatId, texto);
    } catch (err) {
        if (err.response && err.response.error_code === 429) {
            const delay = err.response.parameters.retry_after * 1000;
            console.log(`Esperando ${delay}ms...`);
            await new Promise(res => setTimeout(res, delay));
            return enviarComRetry(ctx, texto);
        } else {
            console.log("Erro ao enviar:", err);
        }
    }
}

// ⏳ Finalizar lote (UMA mensagem)
function finalizarServico(ctx) {
    batch_control.timer = setTimeout(async () => {
        const texto = `✅ Serviço completo\n📥 Processados: ${batch_control.processados}\n♻️ Repetidos: ${batch_control.repetidos}`;
        
        await enviarComRetry(ctx, texto);

        batch_control = {
            timer: null,
            repetidos: 0,
            processados: 0,
            chatId: null
        };
    }, 5000);
}

// 🧹 deletar mensagem depois
function deletarDepois(ctx, msgId, tempo) {
    setTimeout(() => {
        ctx.deleteMessage(msgId).catch(() => {});
    }, tempo * 1000);
}

// 📌 /iniciar
bot.command('iniciar', async (ctx) => {
    let msg = await ctx.reply("💻 Para iniciar o bot:\n\n`node index.js`", { parse_mode: 'Markdown' });
    deletarDepois(ctx, msg.message_id, 60);
});

// 📌 /planilha
bot.command('planilha', async (ctx) => {
    if (fs.existsSync(NOME_PLANILHA)) {
        let msg = await ctx.replyWithDocument(
            { source: NOME_PLANILHA },
            { caption: "📊 Planilha atualizada (apaga em 1 min)" }
        );
        deletarDepois(ctx, msg.message_id, 60);
    } else {
        let msg = await ctx.reply("❌ Nenhuma planilha ainda.");
        deletarDepois(ctx, msg.message_id, 10);
    }
});

// 📥 Processar arquivos
bot.on('document', async (ctx) => {
    const doc = ctx.message.document;

    if (!doc.file_name.endsWith('.txt')) return;

    batch_control.chatId = ctx.chat.id;

    if (batch_control.timer) clearTimeout(batch_control.timer);

    const fileLink = await ctx.telegram.getFileLink(doc.file_id);
    const tempFile = `temp_${doc.file_unique_id}.txt`;

    const res = await fetch(fileLink.href);
    const buffer = await res.arrayBuffer();
    fs.writeFileSync(tempFile, Buffer.from(buffer));

    try {
        let conteudo = fs.readFileSync(tempFile, 'utf-8');

        let dados = extrairDados(conteudo);

        let novo = salvarExcel(dados);

        if (!novo) {
            batch_control.repetidos++;
        } else {
            batch_control.processados++;
        }

        // apagar mensagem do usuário
        ctx.deleteMessage().catch(() => {});

    } catch (e) {
        console.log("Erro:", e);
    }

    fs.removeSync(tempFile);

    finalizarServico(ctx);
});

// 🚀 iniciar bot
bot.launch();
console.log("Bot rodando...");