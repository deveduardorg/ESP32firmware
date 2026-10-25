# libs Standard - Já instaladas dentro do pyhton
import json
import sqlite3  # Carrega o driver do SQLite, um DB Serverless - toda a base vive dentro de um arquivo local
import threading  # Permite qu eo script seja multitask, podendo escutar o servidor do ESP e responder à Página

# libs third-party - não vem instaladas dentro do python, precisam ser baixadas do repositório
# oficial PyPI, utilizando o gerenciador de pacotes pip
import paho.mqtt.client as mqtt
from flask import Flask, jsonify

# Flask é a classe construtora que transforma o script em um server HTTP
# jsonify é uma função utilitária que onverte o dict do python em JSON
from flask_cors import CORS

# Navegadores modernos possuem uma trava de segurança
# chamada Same-Origin Policy. Ao abrir um arquivo index.html e tentar puxar dados de
# http://localhost:5000, o navegador bloqueia a conexão achando que é um ataque hacker
# se o servidor não enviar um cabeçalho de autorização chamado CORS (Cross-Origin Resource Sharing).
#  Essa biblioteca adiciona esse cabeçalho automaticamente.

# ============================================================================
# 1. CONFIGURAÇÃO DO BANCO DE DADOS SQLITE E MUTEX (THREAD LOCK)
# ============================================================================

ARQUIVO_BANCO = "esp32.db"  # definição do nome do arquivo físico local que será criado a mesma pasta do script
TOPICO_MQTT = "faculdade/meuProjeto/temperatura"  # Url da pagina web

# Mesma lista dos 3 servidores que está na tupla do ESP32

# Uma tupla de dicts, cada item da lista guarda o nome 'amigável', endereço-host e porta (1883)
BROKERS_MQTT = [
    {"nome": "HiveMQ", "host": "broker.hivemq.com", "porta": 1883},
    {"nome": "Mosquitto", "host": "test.mosquitto.org", "porta": 1883},
    {"nome": "EMQX", "host": "broker.emqx.io", "porta": 1883},
]

# Cria um cadeado lógico (Mutex) para evitar que duas threads gravem no SQLite ao mesmo tempo

# Pela lógica montada, o script tem 4 fluxos paralelos (Threads) querendo acessar o DB:
# As 3 conexões MQTT tentando gravar INSERTs e a API Flask querendo ler SELECTs par aa página Web
# O SQLite não suporta 2 Threads escrevendo ao mesmo tempo, Caso isso ocorra, um erro fatal é lançado
# e a gravação é corrompida. --> OperationalError: database is locked <--

# threading.lock() cria uma trave logica de exclusão mútua - Se uma thread chegar enqunato a
# porta está trancada, ela aguarda na fila por alguns milissegundos até a trava abrir.
trava_banco = threading.Lock()


# --> CRIANDO A TABELA NO SQLite <--
def inicializar_banco_sqlite():
    """Cria o arquivo telemetria.db e a tabela relacional caso ainda não existam."""

    # O comando with tranca o cadeado trava_banco antes de rodar o cod identado abaixo, e
    # garante que a trava será destrancada no final, mesmo se ocorrer algum erro.
    with trava_banco:
        # Pede a SO para abrir o arquivo esp32.db. Se o arq não exixtir, o connect() o cria.
        # cursor() pega o cmd SQL, entrega para o DB executar, e devolve as linhas de dados uma a uma.
        conexao = sqlite3.connect(ARQUIVO_BANCO)
        cursor = conexao.cursor()

        # Cria a tabela com todas as métricas enviadas pelo JSON do ESP32
        # cursor.execute() envia um CMD DDL - Data Definition Language para o SQLite

        # [...] IF TABLE NOT EXISTS - Só cria a tabela na 1a vez que o script rodar. Se já estiver criada,
        # o CMD é ignorado, sem apagar os dados antigos

        # id INTEGER PRIMARY KEY AUTOINCREMENT:
        # Cria a Chave Primária numérica que sobe sozinha a cada linha (1, 2, 3, 4...).

        # data_hora DATETIME DEFAULT (datetime('now', 'localtime')): O ESP32, por si só, não envia a hora
        # nem a data no JSON? Quando uma linha nova chega no SQLite sem data informada, o próprio banco
        # executa datetime('now', 'localtime'), pegando a data e a hora exatas do relógio do SO e carimbando
        # na coluna automaticamente!
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS esp32.db (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_hora DATETIME DEFAULT (datetime('now', 'localtime')),
                device_id TEXT NOT NULL,
                broker_ativo TEXT,
                temperatura_c REAL,
                umidade_ur REAL,
                pressao_hpa REAL,
                luminosidade_pct REAL,
                gases_pct REAL,
                qualidade_ar TEXT
            )
        """)

        # O cmd conexao.commit() confirma a transação feita com o conexao.execute() [gavado como rascunho na RAM]
        # e grava fisicamente os dados no HD/SSD.
        conexao.commit()

        # Fecha o arquivo do SO, liberando-o para a proxima operação
        conexao.close()
        print(f"[SQLITE OK] Banco de dados '{ARQUIVO_BANCO}' pronto e verificado.")


# ============================================================================
# 2. CALLBACKS DO MQTT (RECEBIMENTO E GRAVAÇÃO NO SQLITE)
# ============================================================================


# Escreve-se a função e  entrega-se seu nome a lib pahi-mqtt. Quando um pacote vindo do ESP32 bater
# na placa de rede do pc, a propria lib 'acorda' a função.
def ao_conectar_broker(client, userdata, flags, rc):
    # Os 4 parâmetros (client, userdata, flags, rc): São exigidos pelo padrão da biblioteca paho-mqtt

    # client - o objeto da execução que acabou de se conectar
    # userdata- 'mochila de dados extra', que guarda os valores da tupla de dicts BROKERS_MQTT
    # flags - Sinalizadores internos do broker
    # rc - Return Code: Num int que indica se a conexão deu certo. No protocolo MQTT rc == 0 é success
    # qualquer numero diferente disso é ERROR

    """Dispara automaticamente quando o computador consegue conectar em um dos 3 brokers."""
    nome_broker = userdata["nome"]
    if rc == 0:
        print(
            f"[MQTT PC CONECTADO] Escutando tópico em {nome_broker} ({userdata['host']})"
        )
        client.subscribe(TOPICO_MQTT)
    else:
        print(f"[MQTT PC AVISO] Código de retorno {rc} ao conectar em {nome_broker}")


def ao_receber_json_do_esp32(client, userdata, msg):
    """
    Dispara no exato milissegundo em que o ESP32 publica um JSON em QUALQUER UM dos 3 brokers.
    Desempacota o JSON e faz o INSERT no arquivo telemetria.db do SQLite.
    """
    nome_broker_receptor = userdata["nome"]

    try:
        # Quando um pacote do ESP32 chega pela internet, o conteúdo vem guardado dentro de msg.payload
        # em formato de bytes

        # 1. Converte os bytes recebidos da rede para texto UTF-8 e depois para Dicionário Python

        # O caminho inverso do ESP32 é feito:
        # .decode("utf-8) faz o conteúdo recebido virar um 'texto-json'; json.loads() transforma a str
        # de volta num dict na var dados
        texto_json = msg.payload.decode("utf-8")
        dados = json.loads(texto_json)

        # 2. Usa o cadeado Mutex para abrir o arquivo SQLite, inserir a linha e fechar com segurança
        with trava_banco:
            conexao = sqlite3.connect(ARQUIVO_BANCO)
            cursor = conexao.cursor()

            cursor.execute(
                # Query Parametrizada - proteção contra SQL Injection - o DB é imune a uma injeção de
                # comando sql malicioso, como um DROP TABLE esp32.db
                # ao usar ? o DB trata ada item estritamente como valor puro
                # or que usaar dados.get("temperatura_c", 0.0) em vez de dados["temperatura_c"]?
                # Em Python, ao tentar acessar dados["temperatura_c"] e, por algum motivo, o pacote JSON
                # vier sem essa chave, o programa lança um erro (KeyError).
                # O método .get("chave", valor_padrao) é seguro: se a chave existir, ele devolve o valor
                # dela; se não existir, ele devolve o valor padrão (0.0) sem dar erro.
                """
                INSERT INTO esp32.db (
                    device_id, broker_ativo, temperatura_c, umidade_ur,
                    pressao_hpa, luminosidade_pct, gases_pct, qualidade_ar
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    dados.get("device_id", "esp32_desconhecido"),
                    dados.get("broker_ativo", nome_broker_receptor),
                    dados.get("temperatura_c", 0.0),
                    dados.get("umidade_ur", 0.0),
                    dados.get("pressao_hpa", 0.0),
                    dados.get("luminosidade_pct", 0.0),
                    dados.get("gases_pct", 0.0),
                    dados.get("qualidade_ar", "NORMAL"),
                ),
            )

            conexao.commit()
            # cursor.lastrowid captura o ultimo id gerado pelo autoincrement
            id_gerado = cursor.lastrowid
            conexao.close()

        print(
            f"[SQLITE INSERT #{id_gerado}] Recebido via {nome_broker_receptor} | Temp: {dados.get('temperatura_c')}°C | Pres: {dados.get('pressao_hpa')}hPa"
        )

    except Exception as erro:
        print(
            f"[ERRO INGESTÃO] Falha ao salvar pacote de {nome_broker_receptor}: {erro}"
        )


def iniciar_escuta_nos_3_brokers():
    """Cria 3 threads em segundo plano, uma para vigiar cada broker da tupla do ESP32."""
    for info_broker in BROKERS_MQTT:
        # Passamos o dicionário info_broker no parâmetro userdata para sabermos quem recebeu a mensagem
        cliente_pc = mqtt.Client(userdata=info_broker)

        # ao escrever o nome da função sem (), apenas aponta-se o endereço da função para o client_pc
        # dizendo: 'guarde o endereço dessa fn e só a execute qunado a conexão for estabelecida'
        cliente_pc.on_connect = ao_conectar_broker
        cliente_pc.on_message = ao_receber_json_do_esp32

        try:
            # connect_async + loop_start fazem a conexão rodar em background sem travar o programa
            cliente_pc.connect_async(
                info_broker["host"], info_broker["porta"], keepalive=60
            )
            # O método loop_start() cria uma Thread separada em 2o plano para aquele broker. Como
            # o laço for gira 3 vezes, 3 Threads são criadas, vigiando simultaneamente os 3 servidores
            cliente_pc.loop_start()
        except Exception as e:
            print(
                f"[ERRO THREAD MQTT] Não foi possível iniciar thread para {info_broker['nome']}: {e}"
            )


# ============================================================================
# 3. API WEB LOCAL (PARA FALLBACK DA PÁGINA WEB E CONEXÃO DO POWER BI)
# ============================================================================

app = Flask(__name__)
CORS(
    app
)  # Libera o acesso para que o arquivo index.html possa fazer fetch() sem erro de CORS
# --> A instanciação do sevrer Wer é criada, utilizando o nome do arquivo atual (__name__)
# e ativa a permissão CORS para o navegador conseguir fazer a comunicação assincrona.

# @app.route("/api/ultima-leitura", methods=["GET"]): O símbolo @ em Python é um Decorador
# (Decorator). Ele transforma a função obter_ultima_leitura() logo abaixo dele em um Endpoint de Rede:
# toda vez que o navegador ou o Watchdog acessar http://localhost:5000/api/ultima-leitura,
# o Flask executa essa função e devolve o retorno dela pela rede!


@app.route("/api/ultima-leitura", methods=["GET"])
def obter_ultima_leitura():
    """
    Rota de Fallback para a Página Web: se todos os WebSockets caírem no navegador,
    a Página Web chama http://localhost:5000/api/ultima-leitura e lê direto do SQLite!
    """
    with trava_banco:
        conexao = sqlite3.connect(ARQUIVO_BANCO)

        # --> Serventia do .row_factory = sqlite3.Row <--
        # Por padrão, ao fazer um SELECT no SQLite, ele devolve apenas uma tupla sem os nomes das
        # colunas. Ex.: (1, '2026-10-21 19:30:00', 'esp32_246f', 'broker.hivemq.com', 25.4, ...).
        # Se isso foose para a Página Web, o JavaScript não saberia quem é temperatura ou pressão, etc
        # Ao configurar row_factory = sqlite3.Row, o SQLite passa a devolver um objeto inteligente
        # que amarra o nome da coluna ao valor, permitindo fazer dict(linha) para convertê-lo
        # instantaneamente em um Dicionário Python com chaves nomeadas!

        conexao.row_factory = (
            sqlite3.Row
        )  # Permite converter a linha do banco direto em Dicionário
        cursor = conexao.cursor()

        # ORDER BY id DESC: Ordena a tabela de trás para frente (do último id gravado para o primeiro).
        # LIMIT 1: Pega apenas a linha do topo (a leitura mais recente de todas).
        # cursor.fetchone(): Busca essa única linha.

        cursor.execute("SELECT * FROM esp32.db ORDER BY id DESC LIMIT 1")
        linha = cursor.fetchone()
        conexao.close()

    if linha:
        # return jsonify(dict(linha)), 200: Converte o dicionário em texto JSON HTTP e envia
        # acompanhado do código de status HTTP 200 (código padrão universal para OK / Sucesso),
        # ou 404 (Not Found) caso o banco ainda esteja vazio.
        return jsonify(dict(linha)), 200
    else:
        return jsonify(
            {
                "mensagem": "Banco SQLite ainda está vazio. Aguardando primeiro envio do ESP32."
            }
        ), 404


# --> Logica do PoweBi <--
@app.route("/api/historico", methods=["GET"])
def obter_historico_completo():
    """
    Rota pronta para o Power BI Desktop!
    Basta ir em 'Obter Dados -> Web' no Power BI e colar: http://localhost:5000/api/historico
    """
    with trava_banco:
        conexao = sqlite3.connect(ARQUIVO_BANCO)
        conexao.row_factory = sqlite3.Row
        cursor = conexao.cursor()

        cursor.execute("SELECT * FROM telemetria_iot ORDER BY id DESC LIMIT 1000")

        # cursor.fetchall(): Em vez de pegar apenas 1 linha (fetchone), pega até 1.000 linhas de histórico de uma vez.
        # [dict(item) for item in cursor.fetchall()]: Isso é uma Compreensão de Lista (List Comprehension)
        # do Python. Ela percorre cada linha retornada pelo banco (for item in ...), transforma cada uma
        # em um dicionário (dict(item)) e monta uma lista completa de JSON pronta para virar tabela no
        # Power BI!
        linhas = [dict(item) for item in cursor.fetchall()]
        conexao.close()

    return jsonify(linhas), 200


# ============================================================================
# 4. PONTO DE PARTIDA DO SERVIDOR
# ============================================================================


# REVER

if __name__ == "__main__":
    print("======================================================")
    print(" INICIANDO SERVIDOR IOT MULTI-BROKER + SQLITE + FLASK ")
    print("======================================================")
    inicializar_banco_sqlite()
    iniciar_escuta_nos_3_brokers()

    # Inicia a API do SQLite na porta 5000 do seu computador
    print(
        "\n[API ATIVA] Fallback da Página Web: http://localhost:5000/api/ultima-leitura"
    )
    print("[API ATIVA] Endpoint para Power BI: http://localhost:5000/api/historico\n")
    app.run(host="0.0.0.0", port=5000, debug=False)
