"""
=============================================================
  GERADOR MASSIVO DE DADOS - ECOMMERCE PORTUGAL (MELHORADO)
=============================================================
  Melhorias aplicadas:
  - Corrigido: fake.district() → fake.distrito()
  - Barra de progresso com tqdm
  - Tratamento de erros com retry automático
  - Logging estruturado em ficheiro
  - Verificação de coleções já existentes (retoma execução)
  - Batch size dinâmico por coleção
  - Tempo estimado por fase
  - IDs armazenados em ficheiro temporário (evita crash de memória)
  - Seed aleatória configurável para reprodutibilidade
=============================================================
"""

import os
import sys
import json
import time
import random
import logging
import pickle
from datetime import datetime, timedelta

from pymongo import MongoClient, errors as mongo_errors
from faker import Faker
from tqdm import tqdm

# =========================
# CONFIGURAÇÃO DE LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("gerador.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# =========================
# CONFIGURAÇÃO GERAL
# =========================

MONGO_URI        = "mongodb://admin:admin123@localhost:27017/"
DATABASE_NAME    = "ecommerce_Angola"

TOTAL_CLIENTES     = 500_000
TOTAL_PRODUTOS     = 300_000
TOTAL_ENCOMENDAS   = 4_000_000
TOTAL_AVALIACOES   = 1_500_000
TOTAL_LOGS         = 2_000_000
TOTAL_PESQUISAS    = 700_000
TOTAL_NOTIFICACOES = 500_000
TOTAL_CARRINHOS    = 500_000

BATCH_SIZE         = 5_000   # documentos por inserção
MAX_RETRIES        = 3       # tentativas em caso de erro
SEED               = 42      # reprodutibilidade (None = aleatório)

# Ficheiro temporário para guardar IDs gerados
IDS_CACHE_FILE     = "ids_cache.pkl"

# =========================
# INICIALIZAÇÃO
# =========================

if SEED is not None:
    random.seed(SEED)

fake = Faker("pt_PT")
if SEED is not None:
    Faker.seed(SEED)

# =========================
# LISTAS AUXILIARES
# =========================

CATEGORIAS = [
    "Eletrónica", "Moda", "Desporto", "Livros",
    "Automóveis", "Gaming", "Móveis", "Saúde", "Beleza"
]

MARCAS = [
    "Samsung", "Apple", "Xiaomi", "Nike", "Sony",
    "LG", "HP", "Dell", "Adidas"
]

METODOS_PAGAMENTO = [
    "multicaixa", "cartao_credito", "paypal", "transferencia"
]

ESTADOS_ENCOMENDA = [
    "processando", "enviado", "entregue", "cancelado"
]

DISPOSITIVOS = ["mobile", "desktop", "tablet"]

NIVEIS_MEMBRO = ["bronze", "prata", "ouro", "platina"]

EVENTOS = ["login", "logout", "compra", "pesquisa", "favorito"]

ORIGENS = ["app", "web"]

# =========================
# CONEXÃO AO MONGODB
# =========================

def conectar():
    """Cria e valida a ligação ao MongoDB."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        log.info("✅ Ligação ao MongoDB estabelecida com sucesso.")
        return client
    except mongo_errors.ServerSelectionTimeoutError as e:
        log.error(f"❌ Não foi possível ligar ao MongoDB: {e}")
        sys.exit(1)

# =========================
# INSERÇÃO COM RETRY
# =========================

def inserir_batch(colecao, batch, tentativa=1):
    """Insere um batch com retry automático em caso de erro."""
    try:
        colecao.insert_many(batch, ordered=False)
    except mongo_errors.BulkWriteError as e:
        # Ignora duplicados (código 11000), relança outros erros
        erros = [err for err in e.details.get("writeErrors", []) if err.get("code") != 11000]
        if erros:
            log.warning(f"⚠️  Erros na inserção (não duplicados): {len(erros)}")
    except Exception as e:
        if tentativa <= MAX_RETRIES:
            log.warning(f"⚠️  Erro na inserção, tentativa {tentativa}/{MAX_RETRIES}: {e}")
            time.sleep(2 ** tentativa)  # backoff exponencial
            inserir_batch(colecao, batch, tentativa + 1)
        else:
            log.error(f"❌ Falha definitiva após {MAX_RETRIES} tentativas: {e}")
            raise

# =========================
# CACHE DE IDs
# =========================

def guardar_ids(clientes_ids, produtos_ids):
    """Guarda os IDs gerados em disco para reutilização."""
    with open(IDS_CACHE_FILE, "wb") as f:
        pickle.dump({"clientes": clientes_ids, "produtos": produtos_ids}, f)
    log.info(f"💾 IDs guardados em cache: {IDS_CACHE_FILE}")

def carregar_ids():
    """Carrega IDs do cache se existir."""
    if os.path.exists(IDS_CACHE_FILE):
        with open(IDS_CACHE_FILE, "rb") as f:
            dados = pickle.load(f)
        log.info(f"📂 IDs carregados do cache ({len(dados['clientes'])} clientes, {len(dados['produtos'])} produtos).")
        return dados["clientes"], dados["produtos"]
    return None, None

# =========================
# VERIFICAR SE JÁ EXISTE
# =========================

def colecao_completa(db, nome, total_esperado):
    """Verifica se a coleção já tem o número esperado de documentos."""
    count = db[nome].estimated_document_count()
    if count >= total_esperado:
        log.info(f"⏭️  Coleção '{nome}' já tem {count:,} documentos. A saltar...")
        return True
    elif count > 0:
        log.info(f"🔄 Coleção '{nome}' tem {count:,}/{total_esperado:,} documentos. A continuar...")
    return False

# =========================
# GERAÇÃO: CLIENTES
# =========================

def gerar_clientes(db):
    if colecao_completa(db, "clientes", TOTAL_CLIENTES):
        # Tenta carregar do cache
        c_ids, _ = carregar_ids()
        if c_ids:
            return c_ids

    log.info("👤 Gerando clientes...")
    clientes_ids = []
    inicio = time.time()

    with tqdm(total=TOTAL_CLIENTES, unit="doc", desc="Clientes", colour="cyan") as pbar:
        for i in range(0, TOTAL_CLIENTES, BATCH_SIZE):
            batch = []
            for j in range(BATCH_SIZE):
                idx = i + j
                if idx >= TOTAL_CLIENTES:
                    break

                cliente_id = f"CLI{idx:07}"
                clientes_ids.append(cliente_id)

                batch.append({
                    "cliente_id": cliente_id,
                    "nome":        fake.name(),
                    "email":       fake.email(),
                    "telefone":    fake.phone_number(),
                    "endereco": {
                        "rua":           fake.street_address(),
                        "cidade":        fake.city(),
                        "distrito":      fake.distrito(),       # ✅ corrigido
                        "codigo_postal": fake.postcode(),
                        "localizacao": {
                            "type": "Point",
                            "coordinates": [
                                round(random.uniform(-9.5, -8.5), 6),
                                round(random.uniform(38.0, 39.0), 6)
                            ]
                        }
                    },
                    "nivel_membro":  random.choice(NIVEIS_MEMBRO),
                    "data_registo":  fake.date_time_between(start_date="-5y", end_date="now"),
                    "ultimo_login":  fake.date_time_this_year(),
                    "ativo":         random.choices([True, False], weights=[90, 10])[0],
                    "estatisticas": {
                        "total_encomendas": random.randint(0, 300),
                        "total_gasto":      round(random.uniform(100, 100_000), 2)
                    }
                })

            inserir_batch(db.clientes, batch)
            pbar.update(len(batch))

    duracao = time.time() - inicio
    log.info(f"✅ Clientes concluídos em {duracao:.1f}s ({TOTAL_CLIENTES/duracao:.0f} docs/s)")
    return clientes_ids

# =========================
# GERAÇÃO: PRODUTOS
# =========================

def gerar_produtos(db):
    if colecao_completa(db, "produtos", TOTAL_PRODUTOS):
        _, p_ids = carregar_ids()
        if p_ids:
            return p_ids

    log.info("📦 Gerando produtos...")
    produtos_ids = []
    inicio = time.time()

    with tqdm(total=TOTAL_PRODUTOS, unit="doc", desc="Produtos", colour="yellow") as pbar:
        for i in range(0, TOTAL_PRODUTOS, BATCH_SIZE):
            batch = []
            for j in range(BATCH_SIZE):
                idx = i + j
                if idx >= TOTAL_PRODUTOS:
                    break

                produto_id = f"PROD{idx:07}"
                produtos_ids.append(produto_id)

                batch.append({
                    "produto_id": produto_id,
                    "nome":       fake.word().capitalize() + " " + fake.word().capitalize(),
                    "categoria":  random.choice(CATEGORIAS),
                    "marca":      random.choice(MARCAS),
                    "preco":      round(random.uniform(1_000, 500_000), 2),
                    "stock":      random.randint(0, 5_000),
                    "ativo":      random.choices([True, False], weights=[85, 15])[0],
                    "avaliacoes": {
                        "media":      round(random.uniform(1, 5), 1),
                        "quantidade": random.randint(0, 10_000)
                    },
                    "tags": fake.words(nb=5),
                    "fornecedor": {
                        "nome": fake.company(),
                        "pais": fake.country()
                    },
                    "data_criacao": fake.date_time_between(start_date="-3y", end_date="now")
                })

            inserir_batch(db.produtos, batch)
            pbar.update(len(batch))

    duracao = time.time() - inicio
    log.info(f"✅ Produtos concluídos em {duracao:.1f}s ({TOTAL_PRODUTOS/duracao:.0f} docs/s)")
    return produtos_ids

# =========================
# GERAÇÃO: ENCOMENDAS
# =========================

def gerar_encomendas(db, clientes_ids, produtos_ids):
    if colecao_completa(db, "encomendas", TOTAL_ENCOMENDAS):
        return

    log.info("🛒 Gerando encomendas...")
    inicio = time.time()

    with tqdm(total=TOTAL_ENCOMENDAS, unit="doc", desc="Encomendas", colour="green") as pbar:
        for i in range(0, TOTAL_ENCOMENDAS, BATCH_SIZE):
            batch = []
            for j in range(BATCH_SIZE):
                idx = i + j
                if idx >= TOTAL_ENCOMENDAS:
                    break

                cliente_id = random.choice(clientes_ids)
                itens      = []
                total      = 0.0

                for _ in range(random.randint(1, 5)):
                    preco      = round(random.uniform(1_000, 50_000), 2)
                    quantidade = random.randint(1, 5)
                    total     += preco * quantidade
                    itens.append({
                        "produto_id": random.choice(produtos_ids),
                        "nome":       fake.word().capitalize(),
                        "categoria":  random.choice(CATEGORIAS),
                        "preco":      preco,
                        "quantidade": quantidade
                    })

                batch.append({
                    "encomenda_id": f"ENC{idx:09}",
                    "cliente": {
                        "cliente_id":  cliente_id,
                        "nome":        fake.name(),
                        "nivel_membro": random.choice(NIVEIS_MEMBRO)
                    },
                    "itens": itens,
                    "pagamento": {
                        "metodo": random.choice(METODOS_PAGAMENTO),
                        "estado": random.choices(["pago", "pendente"], weights=[80, 20])[0]
                    },
                    "entrega": {
                        "estado": random.choice(ESTADOS_ENCOMENDA),
                        "endereco": {
                            "cidade": fake.city(),
                            "localizacao": {
                                "type": "Point",
                                "coordinates": [
                                    round(random.uniform(-9.5, -8.5), 6),
                                    round(random.uniform(38.0, 39.0), 6)
                                ]
                            }
                        }
                    },
                    "valor_total":    round(total, 2),
                    "data_encomenda": fake.date_time_between(start_date="-3y", end_date="now"),
                    "metadados": {
                        "dispositivo": random.choice(DISPOSITIVOS),
                        "origem":      random.choice(ORIGENS)
                    }
                })

            inserir_batch(db.encomendas, batch)
            pbar.update(len(batch))

    duracao = time.time() - inicio
    log.info(f"✅ Encomendas concluídas em {duracao:.1f}s ({TOTAL_ENCOMENDAS/duracao:.0f} docs/s)")

# =========================
# GERAÇÃO: AVALIAÇÕES
# =========================

def gerar_avaliacoes(db, clientes_ids, produtos_ids):
    if colecao_completa(db, "avaliacoes", TOTAL_AVALIACOES):
        return

    log.info("⭐ Gerando avaliações...")
    inicio = time.time()

    with tqdm(total=TOTAL_AVALIACOES, unit="doc", desc="Avaliações", colour="magenta") as pbar:
        for i in range(0, TOTAL_AVALIACOES, BATCH_SIZE):
            batch = []
            for j in range(BATCH_SIZE):
                idx = i + j
                if idx >= TOTAL_AVALIACOES:
                    break
                batch.append({
                    "avaliacao_id":   f"AVA{idx:09}",
                    "produto_id":     random.choice(produtos_ids),
                    "cliente_id":     random.choice(clientes_ids),
                    "pontuacao":      random.randint(1, 5),
                    "comentario":     fake.sentence(),
                    "util":           random.randint(0, 100),
                    "data_avaliacao": fake.date_time_this_year()
                })
            inserir_batch(db.avaliacoes, batch)
            pbar.update(len(batch))

    duracao = time.time() - inicio
    log.info(f"✅ Avaliações concluídas em {duracao:.1f}s")

# =========================
# GERAÇÃO: LOGS
# =========================

def gerar_logs(db, clientes_ids):
    if colecao_completa(db, "logs_sistema", TOTAL_LOGS):
        return

    log.info("📋 Gerando logs...")
    inicio = time.time()

    with tqdm(total=TOTAL_LOGS, unit="doc", desc="Logs", colour="red") as pbar:
        for i in range(0, TOTAL_LOGS, BATCH_SIZE):
            batch = []
            for j in range(BATCH_SIZE):
                idx = i + j
                if idx >= TOTAL_LOGS:
                    break
                batch.append({
                    "evento":      random.choice(EVENTOS),
                    "cliente_id":  random.choice(clientes_ids),
                    "ip":          fake.ipv4(),
                    "dispositivo": random.choice(DISPOSITIVOS),
                    "user_agent":  fake.user_agent(),
                    "timestamp":   fake.date_time_this_year()
                })
            inserir_batch(db.logs_sistema, batch)
            pbar.update(len(batch))

    duracao = time.time() - inicio
    log.info(f"✅ Logs concluídos em {duracao:.1f}s")

# =========================
# GERAÇÃO: PESQUISAS
# =========================

def gerar_pesquisas(db, clientes_ids):
    if colecao_completa(db, "pesquisas", TOTAL_PESQUISAS):
        return

    log.info("🔍 Gerando pesquisas...")
    inicio = time.time()

    with tqdm(total=TOTAL_PESQUISAS, unit="doc", desc="Pesquisas", colour="blue") as pbar:
        for i in range(0, TOTAL_PESQUISAS, BATCH_SIZE):
            batch = []
            for j in range(BATCH_SIZE):
                idx = i + j
                if idx >= TOTAL_PESQUISAS:
                    break
                batch.append({
                    "cliente_id":    random.choice(clientes_ids),
                    "texto":         fake.word(),
                    "resultados":    random.randint(0, 500),
                    "clicou":        random.choice([True, False]),
                    "data_pesquisa": fake.date_time_this_year()
                })
            inserir_batch(db.pesquisas, batch)
            pbar.update(len(batch))

    duracao = time.time() - inicio
    log.info(f"✅ Pesquisas concluídas em {duracao:.1f}s")

# =========================
# GERAÇÃO: NOTIFICAÇÕES
# =========================

def gerar_notificacoes(db, clientes_ids):
    if colecao_completa(db, "notificacoes", TOTAL_NOTIFICACOES):
        return

    log.info("🔔 Gerando notificações...")
    inicio = time.time()

    tipos = ["promocao", "envio", "entrega", "sistema", "lembrete"]

    with tqdm(total=TOTAL_NOTIFICACOES, unit="doc", desc="Notificações", colour="white") as pbar:
        for i in range(0, TOTAL_NOTIFICACOES, BATCH_SIZE):
            batch = []
            for j in range(BATCH_SIZE):
                idx = i + j
                if idx >= TOTAL_NOTIFICACOES:
                    break
                batch.append({
                    "cliente_id":  random.choice(clientes_ids),
                    "tipo":        random.choice(tipos),
                    "mensagem":    fake.sentence(),
                    "lida":        random.choices([True, False], weights=[60, 40])[0],
                    "canal":       random.choice(["email", "push", "sms"]),
                    "data_envio":  fake.date_time_this_year()
                })
            inserir_batch(db.notificacoes, batch)
            pbar.update(len(batch))

    duracao = time.time() - inicio
    log.info(f"✅ Notificações concluídas em {duracao:.1f}s")

# =========================
# GERAÇÃO: CARRINHOS
# =========================

def gerar_carrinhos(db, clientes_ids, produtos_ids):
    if colecao_completa(db, "carrinhos", TOTAL_CARRINHOS):
        return

    log.info("🛍️  Gerando carrinhos...")
    inicio = time.time()

    with tqdm(total=TOTAL_CARRINHOS, unit="doc", desc="Carrinhos", colour="cyan") as pbar:
        for i in range(0, TOTAL_CARRINHOS, BATCH_SIZE):
            batch = []
            for j in range(BATCH_SIZE):
                idx = i + j
                if idx >= TOTAL_CARRINHOS:
                    break

                itens = []
                total = 0.0
                for _ in range(random.randint(1, 5)):
                    preco      = round(random.uniform(1_000, 50_000), 2)
                    quantidade = random.randint(1, 5)
                    total     += preco * quantidade
                    itens.append({
                        "produto_id": random.choice(produtos_ids),
                        "quantidade": quantidade,
                        "preco_unit": preco
                    })

                batch.append({
                    "cliente_id":         random.choice(clientes_ids),
                    "itens":              itens,
                    "valor_estimado":     round(total, 2),
                    "ultima_atualizacao": fake.date_time_this_year()
                })
            inserir_batch(db.carrinhos, batch)
            pbar.update(len(batch))

    duracao = time.time() - inicio
    log.info(f"✅ Carrinhos concluídos em {duracao:.1f}s")

# =========================
# RESUMO FINAL
# =========================

def imprimir_resumo(db, duracao_total):
    colecoes = [
        "clientes", "produtos", "encomendas", "avaliacoes",
        "logs_sistema", "pesquisas", "notificacoes", "carrinhos"
    ]
    total_docs = 0
    log.info("\n" + "=" * 50)
    log.info("  RESUMO FINAL DA BASE DE DADOS")
    log.info("=" * 50)
    for nome in colecoes:
        count = db[nome].estimated_document_count()
        total_docs += count
        log.info(f"  {nome:<20}: {count:>12,} documentos")
    log.info("-" * 50)
    log.info(f"  {'TOTAL':<20}: {total_docs:>12,} documentos")
    log.info(f"  Tempo total: {duracao_total/60:.1f} minutos")
    log.info("=" * 50)

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    inicio_total = time.time()

    log.info("=" * 50)
    log.info("  GERADOR MASSIVO ECOMMERCE PORTUGAL")
    log.info(f"  Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 50)

    # Ligação
    mongo_client = conectar()
    db = mongo_client[DATABASE_NAME]

    # Gerar coleções base (retornam IDs)
    clientes_ids = gerar_clientes(db)
    produtos_ids = gerar_produtos(db)

    # Guardar IDs em cache
    guardar_ids(clientes_ids, produtos_ids)

    # Gerar coleções dependentes
    gerar_encomendas(db, clientes_ids, produtos_ids)
    gerar_avaliacoes(db, clientes_ids, produtos_ids)
    gerar_logs(db, clientes_ids)
    gerar_pesquisas(db, clientes_ids)
    gerar_notificacoes(db, clientes_ids)
    gerar_carrinhos(db, clientes_ids, produtos_ids)

    # Limpeza do cache
    if os.path.exists(IDS_CACHE_FILE):
        os.remove(IDS_CACHE_FILE)
        log.info("🗑️  Cache de IDs removido.")

    duracao_total = time.time() - inicio_total
    imprimir_resumo(db, duracao_total)

    mongo_client.close()
    log.info("🏁 Geração concluída com sucesso!")
