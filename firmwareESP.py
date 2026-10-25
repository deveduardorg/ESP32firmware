import gc  # [Garbage Colector]: libera RAM de objetos temporários (strings/dicts) em momento controlado
import time  # Biblioteca usada para criar delays e contar o tempo

import bmp280  # Biblioteca para ler pressão atmosférica e temperatura no sensor BMP280 via I2C
import dht  # Biblioteca para ler sensores de temperatura DHT.
import machine  # Controla os pinos físicos do ESP32 (entradas, saídas, I2C, ADC).
import network  # Biblioteca que controla a placa de rede do micro
import ssd1306  # O biblioteca responsável por desenhar texto e formas no Display OLED.
import ubinascii  # Biblioteca que converte binários em textos legíveis
import ujson  # Biblioteca nativa para serializar dicionários Python no formato JSON
from micropython import (
    const,  # Constantes resolvidas na compilação (não ocupam RAM nem custam lookup)
)
from umqtt.simple import MQTTClient  # Biblioteca insere a f(x) publisher do protocolo

# ==========================================
# 1. CONFIGURAÇÕES GERAIS E FUNÇÕES MQTT/WIFI
# ==========================================


def clientId():
    macAdress = ubinascii.hexlify(machine.unique_id())

    # machine.unique_id() lê, direto do hardware, o endereço MAC da placa, retornando 6 bytes ilegíveis,
    # como, por ex. b'\x24\x6f\x28\x1a\x2b\x3c'

    # ubinascii.hexlify() converte cada byte em dois carecteres hexadecimais como b'246f281a2b3c'

    # Protocolos de baixo nível como o MQTT trafegam pacotes binários puros pela placa de rede (sockets TCP).
    # Ao passar, uma string normal ("esp32_..."), a cpu precisa gastar ciclos de processamento
    # convertendo de UTF-8 para bytes toda vez que envia um pacote na rede.
    # Declarar constantes de rede já com b"..." (TOPICO_TEMP = b"faculdade/meuProjeto/temperatura")
    # é uma prática de otimização em IoT.

    return b"esp32_" + macAdress


CLIENT_ID = clientId()

# tuplas de contingencia. Se um server falhar, a lista será iterada até todos os brokers
# serem testados

# 1883 é a porta padrão universal para tráfego MQTT sem criptografia. Em automação Industrial real,
# utiliza-se a porta 8883 - MQTT com certificados criptográficos
BROKERS_MQTT = [
    ("broker.hivemq.com", 1883),
    ("test.mosquitto.org", 1883),
    ("broker.emqx.io", 1883),
]

# Canal onde os dados serão postados
TOPICO_TEMP = b"faculdade/meuProjeto/temperatura"

# tuplas de contingencia. Se uma rede falhar, a lista será iterada até todos as redes
# serem testadas
REDES_WIFI = [
    ("Wokwi-GUEST", ""),
    ("redePrincipalUnip", "senhaUnip"),
    ("roteador1", "senha1"),
    ("roteador2", "senha2"),
    ("roteador3", "senha3"),
]

# [AJUSTE] Pinagem real do projeto centralizada em um único lugar (trocar um pino = mudar 1 linha)
PINO_I2C_SDA = const(21)  # SDA do barramento I2C (OLED + BMP280)
PINO_I2C_SCL = const(22)  # SCL do barramento I2C (OLED + BMP280)
PINO_DHT = const(
    15
)  # Dados do DHT. GPIO15 é pino de strapping: se der problema no boot, migre p/ 4, 14 ou 27
PINO_LDR = const(34)  # ADC1, só entrada. Ligar na saída ANALÓGICA (AO) do módulo
PINO_MQ135 = const(
    35
)  # ADC1, só entrada. A saída AO do MQ-135 alimentado com 5V exige divisor de tensão

# [AJUSTE] Classe do sensor DHT. O hardware real do projeto é DHT11; troque por dht.DHT22 se for o caso
SENSOR_DHT_CLASSE = dht.DHT11

# [AJUSTE] 100 kHz tolera melhor protoboard e jumpers longos. Com tudo estável, 400000 reduz o
# tempo do oled.show() de ~90 ms para ~25 ms (esse é o maior bloqueio do loop)
I2C_FREQ_HZ = const(100000)

# [AJUSTE] Isolamento de falhas por sensor
MAX_FALHAS_SENSOR = const(
    3
)  # Falhas seguidas até invalidar o valor (None) e ativar o backoff
BACKOFF_MAX_MS = const(
    20000
)  # Teto do intervalo entre tentativas de um sensor com defeito
INTERVALO_RETRY_HW_MS = const(
    15000
)  # Reinicialização de OLED/BMP280 que falharam no boot
INTERVALO_RETRY_MQTT_MS = const(30000)  # Reconexão ao broker após queda
ADC_AMOSTRAS = const(8)  # Média de leituras ADC (o ADC do ESP32 é ruidoso)
DEBUG_SERIAL = True  # False desliga a linha de telemetria (print é lento e bloqueante)

# [AJUSTE] Aliases locais: evitam procurar time.ticks_* dentro do módulo a cada volta do loop
ticks_ms = time.ticks_ms
ticks_diff = time.ticks_diff
ticks_add = time.ticks_add


# [AJUSTE] Formata valores que podem ser None (sensor sem leitura válida) sem quebrar o print/OLED
def fmt(valor):
    if valor is None:
        return "--"
    return "%.1f" % valor


# [AJUSTE] Arredonda para o JSON preservando None (vira null no JSON, em vez de um falso 0.0)
def arredondar(valor):
    if valor is None:
        return None
    return round(valor, 1)


# [AJUSTE] Até MAX_FALHAS_SENSOR falhas o sensor roda no ritmo normal (falhas isoladas são normais no DHT).
# Depois disso o intervalo dobra a cada falha (4s, 8s, 16s...) até BACKOFF_MAX_MS, para um sensor
# quebrado não consumir tempo de CPU do loop. Ao voltar a funcionar, o ritmo normal é restaurado.
def intervalo_com_backoff(base_ms, falhas):
    if falhas < MAX_FALHAS_SENSOR:
        return base_ms
    return min(base_ms << min(falhas - MAX_FALHAS_SENSOR + 1, 4), BACKOFF_MAX_MS)


# [AJUSTE] Log de falha limitado (1ª, a que ativa o backoff e depois a cada 10), evita inundar o serial
def log_falha(nome, falhas, erro):
    if falhas == 1 or falhas == MAX_FALHAS_SENSOR or falhas % 10 == 0:
        print(f"[ERRO {nome}] Falha #{falhas}: {erro}")


# [AJUSTE] Média de várias amostras do ADC para reduzir o ruído característico do ESP32
def ler_adc_media(adc):
    soma = 0
    for _ in range(ADC_AMOSTRAS):
        soma += adc.read()
    return soma // ADC_AMOSTRAS


def conectarMqtt(lista_brokers, client_id, tentativas_por_broker=2):

    # a cada volta do laço, o Python pega uma tupla como ("broker.hivemq.com", 1883)
    # e separa o primeiro item na variável host e o segundo na variável porta.

    # O parâmetro keepalive=60 é um contrato de vida útil (em segundos) entre o ESP32 e o servidor. Ele diz ao Broker:
    # "Se eu ficar mais de 60 segundos sem mandar nenhuma mensagem ou sinal de vida, feche minha conexão".
    for host, porta in lista_brokers:
        print(f"\n[MQTT] Tentando conectar ao Broker: {host} : {porta}...")
        cliente = MQTTClient(client_id=client_id, server=host, port=porta, keepalive=60)

        for tentativa in range(1, tentativas_por_broker + 1):
            try:
                cliente.connect()
                print(f"[MQTT SUCESSO] Conectado ao Broker {host}")
                return (cliente, host)
            except Exception as erro:
                print(
                    f"[MQTT AVISO] Tentativa {tentativa}/{tentativas_por_broker} em '{host}' falhou: {erro}"
                )
                time.sleep(1.5)

        print(f"[MQTT FAILOVER] Broker '{host}' inacessível. Pulando para o próximo...")

    print("\n[ERRO CRÍTICO] Todos os brokers MQTT configurados estão indisponíveis!")
    return None, None


def conectarWifi(listaRedes, timeout_segundos=10, oled_ref=None):
    # ListaRedes: Permite iterar a tuppla de redes, caso alguma falhe

    # timeout_segundos=10: Define o limite de tempo para tentar a conexão
    # com alguma rede. Passado o tempo, outra item é testado

    # oled_ref: Referencia explicita ao display OLED

    # Instanciação da rede do ESP no modo client/station ==> ESP passa a se comportar
    # como um dispositvo comum, apto à buscar roteadores e conectar-se à uma rede
    # disponível. O oposto é network.AP_IF, onde o ESP cria sua própria rede wifi para
    # outros dispositivos consumirem.

    #  network é um módulo nativo do MicroPython, gerenciando as comunicações de rede
    wlan = network.WLAN(network.STA_IF)

    # liga, fisicamente, o módulo wifi, integrado no ESP
    # O rádio wifi gera picos súbitos de corrente (300mA a 500mA) no momento que ativado.
    # Em projetos alimentado spor USB, esses picos raramente representam problemas.
    # Em projetos industriais, esse consumo pode fazer a DDP cair; assim, o circuito
    # interno de proteção dispara o BrownReset (BOR), reiniciando o micro.
    wlan.active(True)

    # instanciação/consulta do nome da rede (SSID)
    # antes de iniciar o laço de verificação de redes, tal comando verifica se o
    # ssid_atual já corresponde à rede, consultando se já está armazenada em cache, por ex.

    ssid_atual = wlan.config("essid")

    # Verificação de estado de conexão antecipada (Early Return) ==> Se o chip já
    # recebeu um IP do servidor DHCP, o loop de conexão é ignorado, poupando processamento
    # desnecessário da cpu.

    # O método wlan.ifconfig()[0] retorn uma tupla (IP, subnet, gateway, DNS)
    if wlan.isconnected():
        print(f"Conectado à rede {ssid_atual}. IP: {wlan.ifconfig()[0]}")
        return True

    # Laço que itera sobre as credenciais de rede, forçando o micro a buscar por outras alternativas
    # já cadastradas nas tuplas
    for ssid, senha in listaRedes:
        print(f"\nTentando conectar na rede: {ssid}...")

        # [AJUSTE] Encerra qualquer tentativa pendente da rede anterior antes de iniciar a próxima
        wlan.disconnect()
        time.sleep(0.5)

        # Feedback visual no OLED de que está tentando conectar
        if oled_ref:
            oled_ref.fill(0)
            oled_ref.text("Conectando WiFi:", 0, 0)
            oled_ref.text(ssid[:16], 0, 16)
            oled_ref.show()

        # O MicroPython envia um cmd de conexão para a stack de rede subjacente. Trata-se de uma chamada
        # não-bloqueante/assíncrona. O ESP continua a ler sensores, animar o OLED, etc mesmo sem a chamada
        # ter sido respondida

        # O ESP possui um chip controlador de wifi dedicado (um hardware de processamento exclusivo para rede)
        # Esse chip escaneia o canal, negocia a autenticação com o roteador e troca pacotes

        wlan.connect(ssid, senha)

        # Quando o ESP consegue a conexão, geralment roda um cliente NTP (Network Time Protocol) para acertar
        # o relógio interno com servidores globais de horário.

        # Se o relógio estiver atrasado e, de repente, sincronizar com a rede, o valor retornado por um simples
        # time.time() pode saltar no tempo para frente ou trás

        # Caso uma medição de timeout estivesse ocorrendo utilizando time.time() durante a sincronização NTP,
        # o contador sofreria um salto abrupto, fazendo o esp entender um falso estouro de tempo, podendo gerar
        # loops infinitos/bugs

        # A solução para ao problema consiste na implementação de f(x) baseadas em hardware (contadores monotomicos)
        # O contador monotomico é um registrador interno do chip, que acrescenta valores linearmente, com base no
        # clock da cpu, independente de fuso-horário nem de alterações manuais de tempo via NTP. O valor computado
        # não sofre alterações externas, além das calculadas.

        # Como o ticks.ms() retorna um num int de ms, que crese linearmente, ele eventualmente atinge o limite do
        # registrador, zera, e refaz os calculos. para evitar bugs, há o método time.ticks_diff(t1, t2)
        # que calcula a diferença entre dois valores de forma segura.

        inicio_ms = time.ticks_ms()
        timeout_estourou = False

        while not wlan.isconnected():
            tempo_decorrido = time.ticks_diff(time.ticks_ms(), inicio_ms)

            if tempo_decorrido > (timeout_segundos * 1000):
                print(f"\n[AVISO] Timeout na rede '{ssid}'. Partindo para a próxima...")
                timeout_estourou = True
                break

            print(".", end="")
            time.sleep(0.5)

        if not timeout_estourou and wlan.isconnected():
            print(f"\nConectado à rede {ssid}, sem erros.\nIP: {wlan.ifconfig()[0]}")
            if oled_ref:
                oled_ref.fill(0)
                oled_ref.text("WiFi Conectado!", 0, 0)
                oled_ref.show()
            return True

    print("\n[FALHA CRÍTICA] Todas as redes configuradas estão inacessíveis.")
    wlan.active(False)
    return False

    # ==========================================
    # 2. INICIALIZAÇÃO DE HARDWARE (PERIFÉRICOS)
    # ==========================================


# [AJUSTE] Estado do barramento I2C compartilhado (OLED + BMP280). Nasce como None/[] para que as
# funções abaixo nunca encontrem uma variável inexistente (NameError) se a criação do barramento falhar.
i2c = None
enderecos_i2c = []


def iniciar_i2c():
    global i2c, enderecos_i2c
    try:
        # o protocolo I2 é um barramento de comunicação serial que utiliza 2 fios
        # para conectar multiplos dispositivos ao mesmo tempo.

        # SDA - Serial Data (21): Por onde trafegam os dados

        # SCL - Serial Clock (22): Sinal de clock gerado pelo esp para sincronizar
        # a comunicação

        i2c = machine.I2C(
            0,
            sda=machine.Pin(PINO_I2C_SDA),
            scl=machine.Pin(PINO_I2C_SCL),
            freq=I2C_FREQ_HZ,
        )

        # [AJUSTE] Diagnóstico: lista os endereços que responderam (OLED = 0x3c, BMP280 = 0x76 ou 0x77).
        # Lista vazia [] = nenhum dispositivo respondeu (alimentação, fiação ou módulo)
        enderecos_i2c = i2c.scan()
        print(f"[I2C] Dispositivos encontrados: {[hex(d) for d in enderecos_i2c]}")
    except Exception as e:
        i2c = None
        enderecos_i2c = []
        print(f"[ERRO] I2C: {e}")


# A. INSTANCIAÇÃO DO DISPLAY OLED

# Na programação, qunado um erro de hardware acontece, como falta de drivers,
# mau-contato, periféricos danificados, etc; o compilador lança um erro de
# execução e trava. O uso de try-except captura o erro (se exixtir) e dá instruções
# ao controlador sobre o que fazer, impedindo que o Eqpto. fique inoperante


# [AJUSTE] Transformado em função para poder ser chamada de novo pelo loop caso o display
# falhe no boot ou se desconecte depois (devolve o objeto display, ou None se falhar)
def iniciar_oled():
    if i2c is None:
        iniciar_i2c()
    try:
        # Ao instanciar o OLED, o esp reserva um bloco de memoria, funcioando como um 'rascunho mental' da tela,
        # nesse caso, tela = 128 cols x 64 ln ==> 8.192 pixels

        # como o display é monocromatico, cada pixel precisa de apenas 1 bit de dados.
        # convertendo para bytes ==> (8.192 bits / 8 bits) == 1.024  bytes == 1kb de RAM

        display = ssd1306.SSD1306_I2C(128, 64, i2c)

        print("[OK] Display OLED I2C inicializado.")

        # Acende o display imediatamente após instanciar!

        # Quando o micro é reiniciado, a memória interna da RAM do eqpto (framebuffer)
        # pode conter lixo (pixels acesos da sessao anterior). Para evitar isso, o cmd
        # oled.fill(0) limpa a tela, preenchendo-a com pixels pretos/vazios

        # O sistema de coordenadas do micro é diferente do plano cartesiano convencional:
        # Eixo X (Horizontal): Vai do pixel 0 (borda esquerda) até o pixel 127 (borda direita).

        #  Y (Vertical): Vai do pixel 0 (topo da tela) até o pixel 63 (base da tela)
        # quanto maior o Y, mais para baixo o texto desce.

        # Por padrão, cada carecter possui 8px x 8px, totalizando 16 caracteres possiveis por linha

        display.fill(0)
        display.text("Iniciando ESP32...", 0, 0)

        # Tudo o que fill() e text() fizeram aconteceu apenas na memória RAM do ESP32. Quando oled.show() é executado, o ESP32 empacota
        # os 1.024 bytes do Framebuffer e os envia de uma só vez pelos pinos SDA (21) e SCL (22) para o chip controlador do display.
        # A imagem muda instantaneamente, sem piscar.
        display.show()
        return display

    except Exception as e:
        print(f"[ERRO] Display OLED: {e}")
        return None


iniciar_i2c()
oled = iniciar_oled()
oled_ok = oled is not None

# B. INSTANCIAÇÃO DO SENSOR DHT22
# [AJUSTE] A classe vem de SENSOR_DHT_CLASSE (DHT11 no hardware atual). Instanciar o DHT não toca o
# hardware, então só falha se o pino for inválido. sensor_dht = None evita NameError mais adiante.
try:
    sensor_dht = SENSOR_DHT_CLASSE(machine.Pin(PINO_DHT))
    print(f"[OK] Sensor DHT configurado no GPIO {PINO_DHT}.")
except Exception as e:
    sensor_dht = None
    print(f"[ERRO] DHT: {e}")

# B.2 INSTANCIAÇÃO DO SENSOR BMP280 (Compartilhando o mesmo barramento I2C)


# [AJUSTE] Função para poder ser repetida pelo loop. Antes, a lib devolvia 1013.2 hPa mesmo SEM o chip
# conectado (falso positivo). Agora o sensor só é aceito se responder no scan I2C (0x76 ou 0x77).
# Atenção na fiação do BMP280: CSB deve ir ao 3.3V (modo I2C) e SDO ao GND (endereço 0x76). Não deixe flutuando.
def iniciar_bmp():
    try:
        if i2c is None:
            iniciar_i2c()
        presentes = i2c.scan()
        if 0x76 not in presentes and 0x77 not in presentes:
            raise OSError("BMP280 nao respondeu no I2C (0x76/0x77)")
        sensor = bmp280.BMP280(i2c)
        print("[OK] Sensor BMP280 configurado no barramento I2C (Pinos 21/22).")
        return sensor
    except Exception as e:
        print(f"[ERRO] BMP-280: {e}")
        return None


sensor_bmp = iniciar_bmp()
bmp_ok = sensor_bmp is not None

# C. INSTANCIAÇÃO DOS SENSORES ANALÓGICOS (ADC1)

# As portas 34 e 35 pertencem ao ADC1 do ESP32 => Conversor Analógico-Digital.

# A maioria das grandezas fisicas do mundo real variam de forma continua
# (umidade, pressao, temperatura, som, luminosidade), gerando diferentes niveis
# de DDP no micro. Porém o esp trabalha apenas com valores binarios (0 ou 1)

# As portas ADC fazem a ponte de entre o sensor-micro, convertendo o sinal
# captado num numero inteiro.


# Apesar dos outros sensores tbm medirem grandezas analogicas, os demais posssuem
# chips integrados que fazem a conversão direta, sem a necessiade de uma porta
# dedicada para isso.

# O ADC2 tbm é um conversor analógico digital, porém compartilha o memsmo circuito interno que o radio wifi.
# ao ativar os sensoes juntos com o wifi, a leitura analógica falharia, retornando OSError.

# [AJUSTE] ldr/mq135 = None no except: sem isso, uma falha aqui causaria NameError no loop principal
ldr = None
mq135 = None

try:
    ldr = machine.ADC(machine.Pin(PINO_LDR))

    # Por padrão de fábrica (ATTN_0DB), o conversor ADC do ESP32 só consegue medir tensões muito baixas, entre 0V e ~1.1V.
    # Qualquer tensão acima de 1.1V já trava a leitura no valor máximo (4095), causando saturação (clipping).
    # Ao aplicar uma atenuação de 11 decibéis (ATTN_11DB), o circuito interno reduz o sinal de entrada antes de medi-lo, expandindo a faixa de leitura possível

    ldr.atten(machine.ADC.ATTN_11DB)

    # Define a quantidade de bits usada para representar a tensão medida, nesse caso, 12 bits.
    # Niveis Possiveis = 2^(12) = 4096 degraus (0-4095)

    ldr.width(machine.ADC.WIDTH_12BIT)  # Garante que a leitura vá até 4095
except Exception as e:
    ldr = None
    print(f"[ERRO] LDR: {e}")

try:
    mq135 = machine.ADC(machine.Pin(PINO_MQ135))
    mq135.atten(machine.ADC.ATTN_11DB)
    mq135.width(machine.ADC.WIDTH_12BIT)  # Garante que a leitura vá até 4095
except Exception as e:
    mq135 = None
    print(f"[ERRO] MQ-135: {e}")

    # ==========================================
    # 3. ROTINA DE CONEXÃO E LOOP PRINCIPAL
    # ==========================================

print("\n--- INICIANDO CONEXÕES DE REDE ---")

# A variavel conectou_wifi recebe um valor booleano, servindo com flag, indicando se o micro
# consegiu estabelcer a conexão com alguma rede.

# Na linha 68, def conectarWifi(listaRedes, timeout_segundos=10, oled_ref=None), oled_ref é um parametro
# que faz referencia ao OLED. a f(x) foi criada antes da instanciação do oled, por isso o argumento None.
# ao chamar a função posteiormente, o oled é passado como argumento do parametro oled_ref.

# Todo esse processo permite que a função conectarWifi(), que é isolada, saiba que existe um display
# disponivel, pegando o objeto oled global emprestado dentro do bloco de execução local

# REDES_WIFI é o 1o argumento da função criada anteriormente (conectarWifi), dizendo quais redes
# serão utilidas.

# O segundo argumento é um operador ternário, basicamente uma estrutura condicional escrita numa
# só linha; que verifica se o oled está ok.

conectou_wifi = conectarWifi(REDES_WIFI, oled_ref=oled if oled_ok else None)

cliente_mqtt = None

# se o valor retornado pela variavel conectou_wifi foi True, o bloco é executado.
if conectou_wifi:
    # se o oled estiver ok, o bloco é executado.
    if oled_ok:
        oled.fill(0)
        oled.text("Conectando ao MQTT", 0, 0)
        oled.show()

    # a f(x) conectarMqtt devolve uma tupla de valores de um só vez, desempactoadas e recebidas nas varíaveis
    # à esquerda, na ordem. conectarMqtt() recebe como parametros a TUPLA BROKERS_MQTT e CLIENT_ID
    cliente_mqtt, broker_conectado = conectarMqtt(BROKERS_MQTT, CLIENT_ID)

print("\n--- INICIANDO LOOP DE LEITURA DAS MÉTRICAS ---")

# ---------------------------------------
# Maquina de Estados Finitos == FSM
# ---------------------------------------
# Na engenharia, dividimos as FSMs em dois tipos clássicos, criados na década de 1950:

# Máquina de Moore: A saída física (o que aparece no display ou nos pinos)
# depende exclusivamente do Estado Atual. O código usa uma Máquina de Moore: se pagina_display vale 0,
# a tela mostra Clima; se vale 1, mostra Ambiente.

# Máquina de Mealy: A saída física depende tanto do Estado Atual quanto do Evento de Entrada que acabou de
# acontecer.


# Estado inicial da tela - Fora do loop
pagina_display = 0

# =====================================================================
# Configuração dos Timers Assíncronos com time.ticks_ms()
# =====================================================================

# Assim que o ESP é ligado à energia, um contador interno cresce. A função time.ticks_ms() retorna qual numero o cont está


INTERVALO_SENSORES_MS = const(2000)  # Lê sensores a cada 2 segundos (2000 ms)
INTERVALO_DISPLAY_MS = const(
    3000
)  # Alterna a página do OLED a cada 3 segundos (3000 ms)
INTERVALO_MQTT_MS = const(
    5000
)  # Envia o pacote JSON via MQTT a cada 5 segundos (5000 ms)

# Inicializa os marcadores de tempo recuados para forçar a 1ª execução imediatamente no boot
tick_inicial = ticks_ms()


# Guarda em cada variável qual foi a ultima vez que cada tarefa rodou
# [AJUSTE] Cada sensor ganhou o seu próprio timer (e o seu próprio intervalo, que cresce se ele falhar).
# Assim um sensor lento ou defeituoso nunca atrasa nem cancela a leitura dos outros.
ultimo_tick_dht = ticks_add(tick_inicial, -INTERVALO_SENSORES_MS)
ultimo_tick_bmp = ticks_add(tick_inicial, -INTERVALO_SENSORES_MS)
ultimo_tick_ldr = ticks_add(tick_inicial, -INTERVALO_SENSORES_MS)
ultimo_tick_mq135 = ticks_add(tick_inicial, -INTERVALO_SENSORES_MS)
ultimo_tick_console = ticks_add(tick_inicial, -INTERVALO_SENSORES_MS)
ultimo_tick_display = ticks_add(tick_inicial, -INTERVALO_DISPLAY_MS)
ultimo_tick_mqtt = ticks_add(tick_inicial, -INTERVALO_MQTT_MS)
ultimo_tick_retry_hw = (
    tick_inicial  # Primeira tentativa de religar hardware só após INTERVALO_RETRY_HW_MS
)
ultimo_tick_retry_mqtt = tick_inicial  # Idem para a reconexão MQTT

# [AJUSTE] Intervalo atual de cada sensor (começa no normal e sobe com o backoff) e contadores de falhas seguidas
intervalo_dht_ms = INTERVALO_SENSORES_MS
intervalo_bmp_ms = INTERVALO_SENSORES_MS
intervalo_ldr_ms = INTERVALO_SENSORES_MS
intervalo_mq135_ms = INTERVALO_SENSORES_MS
falhas_dht = 0
falhas_bmp = 0
falhas_ldr = 0
falhas_mq135 = 0

# Variáveis de estado das medições inicializadas em escopo global
# [AJUSTE] None = "sem leitura válida" (diferente de 0.0, que seria um valor real). Vai como null no JSON.
temp = None
umid = None
pressao_hpa = None
luz_raw = 0
luz_pct = None
gas_raw = 0
gas_pct = None
status_ar = "SEM DADO"

while True:
    # Captura a referência monotônica de tempo deste giro do loop
    agora_ms = ticks_ms()

    try:
        # =====================================================================
        # TAREFA ASSÍNCRONA 1: LEITURA DOS SENSORES (A cada 2000 ms)
        # =====================================================================
        # [AJUSTE] A tarefa 1 foi dividida em 4 sub-tarefas independentes (DHT, BMP280, LDR, MQ-135),
        # cada uma com timer, try/except e backoff próprios.

        # Tempo Atual - Ultima Execução >= Intervalo da Desejado entre Execuções?
        # Se sim, o ESP carimba o horário atual (agora_ms) na variavel ultimo_tick_sensores
        # Se não, pula o if em microsegundos, sem travar a CPU

        # ---------- 1.1 DHT ----------
        if ticks_diff(agora_ms, ultimo_tick_dht) >= intervalo_dht_ms:
            ultimo_tick_dht = agora_ms

            # A. Leitura do DHT22

            # Quando a linha a baixo roda, o pino 15 envia um pulso eletrico de partida ao sensor. O chip faz a medição e devolve
            # # uma resposta em bits pelo memso valor
            # O ESP confere o checksum (soma de verificação para garantir que os bits não foram corrompidos), se ok, salva os valores
            if sensor_dht:
                try:
                    sensor_dht.measure()

                    # Ao rodar temperature() e humidity(), o ESP32 consulta na RAM os ultimos valores medidos com measure(), devolvendo os
                    # valores já convertidos em inteiros
                    temp = sensor_dht.temperature()
                    umid = sensor_dht.humidity()
                    if falhas_dht >= MAX_FALHAS_SENSOR:
                        print("[DHT] Sensor voltou a responder.")
                    falhas_dht = 0
                except Exception as e:
                    falhas_dht += 1
                    log_falha("DHT", falhas_dht, e)
                    if falhas_dht >= MAX_FALHAS_SENSOR:
                        temp = None
                        umid = None
                intervalo_dht_ms = intervalo_com_backoff(
                    INTERVALO_SENSORES_MS, falhas_dht
                )

        # ---------- 1.2 BMP280 ----------
        if ticks_diff(agora_ms, ultimo_tick_bmp) >= intervalo_bmp_ms:
            ultimo_tick_bmp = agora_ms

            # A.2 [NOVO] Leitura do BMP280 (Pressão Atmosférica em hPa)

            # Existem duas versões da lib bmp280. Na versão A, o autor criou pressure como uma função. Para ler, é necessário
            # utlizar .pressure()
            # Na versão B, o autor criou como uma propriedade. Para ler, não se usa parenteses. Se usar, o programa gera um TypeError

            # Para contornar essas divergencias, utiliza-se a função nativa callable() do python (algo que é chamável, portanto uma função)

            if bmp_ok and sensor_bmp:
                try:
                    # Algumas libs bmp280 usam propriedade (.pressure) e outras método (.pressure()). Tratamos ambas:
                    leitura_p = (
                        sensor_bmp.pressure()
                        if callable(sensor_bmp.pressure)
                        else sensor_bmp.pressure
                    )

                    # A pressão atmosférica normal ao nível do mar vale cerca de 101325 Pascais, ou 1013.2 Hectopascais.
                    # Algumas bibliotecas devolvem o número bruto em Pascals (101325.0), que é um número enorme e difícil de ler.
                    # Outras já entregam dividido por 100, em Hectopascais (1013.25).

                    # Se o número lido for maior que 2000 (ex: 101325), está Pascals
                    # Se for menor que 2000 (ex: 1013.25), já está em hPa! Só resta arredondar para 1 casa decimal.
                    leitura_hpa = (
                        round(leitura_p / 100.0, 1)
                        if leitura_p > 2000
                        else round(leitura_p, 1)
                    )

                    # [AJUSTE] Validação de plausibilidade: a pressão na Terra fica entre ~300 e ~1100 hPa.
                    # Fora disso, é lixo de barramento/chip ausente e não pode ir para o JSON como se fosse real.
                    if not (300.0 <= leitura_hpa <= 1100.0):
                        raise ValueError(
                            "pressao fora da faixa fisica: %s" % leitura_hpa
                        )

                    pressao_hpa = leitura_hpa
                    if falhas_bmp >= MAX_FALHAS_SENSOR:
                        print("[BMP280] Sensor voltou a responder.")
                    falhas_bmp = 0
                except Exception as e:
                    falhas_bmp += 1
                    log_falha("BMP280", falhas_bmp, e)
                    if falhas_bmp >= MAX_FALHAS_SENSOR:
                        pressao_hpa = None
                intervalo_bmp_ms = intervalo_com_backoff(
                    INTERVALO_SENSORES_MS, falhas_bmp
                )

        # Os sensores LDR e MQ-135 alteram sua resistencia em dado momento de suas medições, fazendo variar a
        # DDP que chega nos pinos 34 e 35

        # O micro não entende "2,1 Volts". Ao chmamar ldr.read(), o Conversor Analógico-Digital (ADC) mede a tensão no pino
        # e a converte em um número inteiro proporcional na escala de 12 bits (valores numerados de 0 a 4095).Por isso o sufixo
        # _raw (bruto em inglês) à variável: ela guarda o número puro do conversor (por exemplo, 3071), sem unidade humana.
        # Do Valor Bruto para Porcentagem (luz_pct e gas_pct):Para transformar um número de 0 a 4095 em uma escala intuitiva
        # de 0% a 100%, aplicamos a normalização linear (regra de três): (leitura bruta / 4095) * 100

        # ---------- 1.3 LDR ----------
        if ticks_diff(agora_ms, ultimo_tick_ldr) >= intervalo_ldr_ms:
            ultimo_tick_ldr = agora_ms
            # A.3 Leitura dos Analógicos LDR e MQ-135
            if ldr:
                try:
                    luz_raw = ler_adc_media(ldr)
                    luz_pct = round((luz_raw / 4095) * 100, 1)
                    falhas_ldr = 0
                except Exception as e:
                    falhas_ldr += 1
                    log_falha("LDR", falhas_ldr, e)
                    if falhas_ldr >= MAX_FALHAS_SENSOR:
                        luz_pct = None
                intervalo_ldr_ms = intervalo_com_backoff(
                    INTERVALO_SENSORES_MS, falhas_ldr
                )

        # ---------- 1.4 MQ-135 ----------
        if ticks_diff(agora_ms, ultimo_tick_mq135) >= intervalo_mq135_ms:
            ultimo_tick_mq135 = agora_ms
            if mq135:
                try:
                    gas_raw = ler_adc_media(mq135)
                    gas_pct = round((gas_raw / 4095) * 100, 1)
                    falhas_mq135 = 0
                except Exception as e:
                    falhas_mq135 += 1
                    log_falha("MQ-135", falhas_mq135, e)
                    if falhas_mq135 >= MAX_FALHAS_SENSOR:
                        gas_pct = None
                intervalo_mq135_ms = intervalo_com_backoff(
                    INTERVALO_SENSORES_MS, falhas_mq135
                )

            # Classificação qualitativa da qualidade do ar baseada na saturação do MQ-135
            if gas_pct is None:
                status_ar = "SEM DADO"
            elif gas_pct < 35.0:
                status_ar = "BOM"
            elif gas_pct < 70.0:
                status_ar = "MODERADO"
            else:
                status_ar = "CRITICO!"

        # ---------- 1.5 CONSOLE ----------
        if ticks_diff(agora_ms, ultimo_tick_console) >= INTERVALO_SENSORES_MS:
            ultimo_tick_console = agora_ms

            # D. Exibição no Console Serial
            if DEBUG_SERIAL:
                print(
                    f"[TELEMETRIA] Temp: {fmt(temp)}°C | Umid: {fmt(umid)}%UR | Pres: {fmt(pressao_hpa)}hPa | Luz: {fmt(luz_pct)}% | Gas: {fmt(gas_pct)}% ({status_ar})"
                )

        # =====================================================================
        # TAREFA ASSÍNCRONA 2: ENVIO VIA MQTT EM JSON (A cada 5000 ms)
        # =====================================================================
        # E. Envio via MQTT
        if ticks_diff(agora_ms, ultimo_tick_mqtt) >= INTERVALO_MQTT_MS:
            ultimo_tick_mqtt = agora_ms

            if conectou_wifi and cliente_mqtt:
                try:
                    # Estrutura de dicionário DICT chave-valor mapeada para colunas SQL, Web e Power BI
                    # A chave (à esquerda dos dois pontos, serão os nomes das colunas)
                    # O valor (à direita, são as varíaveis armazenadas nas celulas)

                    payload_dados = {
                        # JSON só aceita tipos de dados básicos (strings/int/float/bool), não aceitando o
                        # typo b""" (bytes). Para resolver isso, o método .decode('utf-8') o converte para uma
                        # string comum, legível ao JSON.
                        "device_id": CLIENT_ID.decode("utf-8"),
                        "temperatura_c": arredondar(temp),
                        "umidade_ur": arredondar(umid),
                        "pressao_hpa": pressao_hpa,
                        "luminosidade_pct": luz_pct,
                        "gases_pct": gas_pct,
                        "qualidade_ar": status_ar,
                    }

                    # A função ujson.dumps() pega o dict da RAM e o converte numa string única, padrão JSON

                    # '{"device_id": "esp32_246f28", "temperatura_c": 25.4,
                    # "umidade_ur": 60.0, "pressao_hpa": 1013.2, "luminosidade_pct": 75.2, "gases_pct": 18.4,
                    # "qualidade_ar": "BOM"}'
                    msg_mqtt = ujson.dumps(payload_dados).encode("utf-8")
                    # A placa de rede wifi só trafega com pacotes binarios puros. Por isso o método .enconde('utf-8')

                    cliente_mqtt.publish(TOPICO_TEMP, msg_mqtt)
                    print(f"[MQTT JSON] Pacote enviado com sucesso: {payload_dados}")

                    # [AJUSTE] Libera a RAM do dict/strings temporários neste ponto (a cada 5 s) e não no meio
                    # de uma tarefa crítica, evitando pausas longas e fragmentação do heap
                    del payload_dados, msg_mqtt
                    gc.collect()
                except Exception as e:
                    print(f"[ERRO MQTT] Falha ao enviar dados: {e}")
                    # [AJUSTE] Socket provavelmente quebrado: descarta o cliente para que a reconexão (abaixo) o refaça
                    try:
                        cliente_mqtt.disconnect()
                    except Exception:
                        pass
                    cliente_mqtt = None

        # [AJUSTE] Reconexão ao broker após queda (ou se todos falharam no boot). Roda a cada 30 s com 1 tentativa
        # por broker. Pode bloquear o loop por alguns segundos se os brokers estiverem fora do ar.
        if (
            conectou_wifi
            and cliente_mqtt is None
            and ticks_diff(agora_ms, ultimo_tick_retry_mqtt) >= INTERVALO_RETRY_MQTT_MS
        ):
            cliente_mqtt, broker_conectado = conectarMqtt(
                BROKERS_MQTT, CLIENT_ID, tentativas_por_broker=1
            )
            ultimo_tick_retry_mqtt = ticks_ms()
            agora_ms = ultimo_tick_retry_mqtt  # a chamada acima pode ter demorado: atualiza a referência de tempo

        # =====================================================================
        # TAREFA ASSÍNCRONA 3: ATUALIZAÇÃO DO DISPLAY OLED - FSM (A cada 3000 ms)
        # =====================================================================
        # F. Atualização do Display OLED (Paginação)
        if ticks_diff(agora_ms, ultimo_tick_display) >= INTERVALO_DISPLAY_MS:
            ultimo_tick_display = agora_ms

            if oled_ok:
                try:
                    oled.fill(0)

                    if pagina_display == 0:
                        # TELA 1: Clima (DHT22 + BMP280) com unidades meteorológicas exatas
                        oled.text("== CLIMA ==", 20, 0)
                        oled.text(f"Temp: {fmt(temp)} C", 0, 16)
                        oled.text(f"Umid: {fmt(umid)} %UR", 0, 32)
                        oled.text(f"Pres:{fmt(pressao_hpa)} hPa", 0, 48)
                        pagina_display = 1
                    else:
                        # TELA 2: Ambiente (LDR + MQ-135) com grandezas claras e diagnóstico
                        oled.text("== AMBIENTE ==", 10, 0)
                        # Formatação :.1f adicionada para evitar strings muito longas no display
                        oled.text(f"Lum : {fmt(luz_pct)} %Luz", 0, 16)
                        oled.text(f"Gas : {fmt(gas_pct)} %Ind", 0, 32)
                        oled.text(f"Ar  : {status_ar}", 0, 48)
                        pagina_display = 0

                    oled.show()
                except Exception as e:
                    # [AJUSTE] Display falhou em runtime (fio solto etc.): desliga só o display, o resto segue
                    # funcionando, e o bloco de retry abaixo tenta religá-lo
                    print(f"[ERRO NO DISPLAY]: {e}")
                    oled_ok = False

        # [AJUSTE] Retry de hardware: tenta religar OLED e BMP280 que falharam (no boot ou em runtime),
        # a cada 15 s. Se o fio for corrigido com a placa ligada, o periférico volta sozinho, sem reiniciar.
        if (not oled_ok or not bmp_ok) and ticks_diff(
            agora_ms, ultimo_tick_retry_hw
        ) >= INTERVALO_RETRY_HW_MS:
            ultimo_tick_retry_hw = agora_ms
            if not oled_ok:
                oled = iniciar_oled()
                oled_ok = oled is not None
            if not bmp_ok:
                sensor_bmp = iniciar_bmp()
                bmp_ok = sensor_bmp is not None
                if bmp_ok:
                    falhas_bmp = 0
                    intervalo_bmp_ms = INTERVALO_SENSORES_MS

    except Exception as e:
        # Alterado de OSError para Exception para capturar e printar qualquer erro que parar o código
        print(f"[ERRO DE LEITURA]: {e}")
        if oled_ok:
            try:
                oled.fill(0)
                oled.text("Erro Leitura", 15, 20)
                oled.text("Verifique Fios", 10, 40)
                oled.show()
            except Exception:
                oled_ok = False

    # Pausa curta de 10ms (0.01s) não-bloqueante para poupar CPU e manter a pilha Wi-Fi responsiva
    # [AJUSTE] O valor real sempre foi 100 ms (o comentário acima diz 10 ms). 100 ms basta: todos os
    # intervalos são múltiplos de 100 ms e a CPU fica ociosa o resto do tempo.
    time.sleep_ms(100)
