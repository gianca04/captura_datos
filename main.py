import os
import time
import logging
import json
import threading
import queue
import ctypes
import signal
import sys
from ctypes import byref, POINTER, c_void_p, c_ubyte
import snap7
from snap7.util import get_real, get_bool
# Resolución de compatibilidad entre diferentes versiones de python-snap7
try:
    # Para versiones modernas (ej. Windows python-snap7 >= 1.x)
    from snap7.types import S7DataItem, Areas, WordLen
except ImportError:
    # Para versiones antiguas (ej. LXC python-snap7 0.11 o inferior)
    from snap7.snap7types import S7DataItem, Areas, WordLen
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

# 1. Configuración de Logging Profesional
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("PLC-MQTT")

# 2. Cargar variables desde el archivo .env
load_dotenv()

PLC_IP = os.getenv("PLC_IP")
PLC_RACK = int(os.getenv("PLC_RACK", 0))
PLC_SLOT = int(os.getenv("PLC_SLOT", 3))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 10))
SENSOR_READ_INTERVAL = float(os.getenv("SENSOR_READ_INTERVAL", 2))

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX", "plc/sensors")
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# 3. Globales para multihilo
# Limitamos la cola a 5000 para evitar OOM (Out Of Memory)
metrics_queue = queue.Queue(maxsize=5000)
running = True

def signal_handler(sig, frame):
    global running
    logging.info("Señal de interrupcion recibida. Apagando sistema...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# 4. Direcciones de memoria del PLC
MARCAS = {
    'FIT_001': {'Flow': (65, 424, 0, 'REAL')},
    'LIT_001': {'Level': (99, 424, 0, 'REAL')},
    'LIT_002': {'Level': (112, 424, 0, 'REAL')},
    'TT_001': {'Temp': (101, 424, 0, 'REAL')},
    'TT_002': {'Temp': (77, 424, 0, 'REAL')},
    'MOTOR_01': {'Running': (97, 446, 0, 'BOOL')},
}

def print_ascii_logo():
    # Definición de colores
    GREEN = "\033[1;32m"
    RESET = "\033[0m"
    
    ascii_art = f"""
{GREEN}  ███████╗ {GREEN}█████╗ {GREEN}████████╗
{GREEN}  ██╔════╝{GREEN}██╔══██╗{GREEN}╚══██╔══╝
{GREEN}  ███████╗{GREEN}███████║   {GREEN}██║   
{GREEN}  ╚════██║{GREEN}██╔══██║   {GREEN}██║   
{GREEN}  ███████║{GREEN}██║  ██║   {GREEN}██║   
{GREEN}  ╚══════╝{GREEN}╚═╝  ╚═╝   {GREEN}╚═╝   
{RESET}
 === GitHub: GianCa04 ===
"""
    print(ascii_art)

def connect_plc() -> snap7.client.Client:
    """Conexión robusta al PLC con reintentos detallados."""
    plc = snap7.client.Client()
    attempt = 1
    while True:
        try:
            logger.info(f"🔌 Intentando conexión al PLC {PLC_IP} (Rack {PLC_RACK}, Slot {PLC_SLOT}) - Intento {attempt}")
            plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)
            if plc.get_connected():
                print_ascii_logo()
                logger.info("✓ PLC Conectado exitosamente")
                return plc
        except Exception as e:
            logger.error(f"X Fallo en conexión PLC: {e}")
        
        attempt += 1
        time.sleep(RETRY_DELAY)

def mqtt_worker():
    """Hilo consumidor: Envía a MQTT con la menor latencia posible."""
    logging.info(f"Iniciando Worker MQTT -> {MQTT_BROKER}")
    
    try:
        # Intentar usar API v2 para evitar warnings en paho-mqtt 2.x
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except (AttributeError, TypeError):
        client = mqtt.Client()
        
    if MQTT_USER and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        
    while running:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 10)
            client.loop_start()
            logging.info("Broker MQTT Conectado")
            break
        except Exception as e:
            logging.error(f"Error Broker: {e}")
            time.sleep(RETRY_DELAY)

    while running:
        try:
            # Usamos timeout para poder verificar 'running' periodicamente
            batch = metrics_queue.get(timeout=1.0)
            if batch is None: break
            
            for topic, equipo, valor, ts in batch:
                # El campo de valor siempre debe ser genérico ('value') para el estándar de Series de Tiempo
                msg = f'{{"value":{valor},"timestamp":{ts}}}'
                # Subimos QoS a 1 (al menos una vez) para asegurar que no se pierdan datos importantes
                client.publish(topic, msg, qos=1)
                
            metrics_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Error publicación: {e}")

    # Cierre limpio del worker MQTT
    logging.info("Deteniendo Worker MQTT de forma limpia...")
    client.loop_stop()
    client.disconnect()

def main():
    # Iniciar worker en background
    threading.Thread(target=mqtt_worker, daemon=True).start()
    
    plc = connect_plc()
    
    # 5. Pre-configuración de lectura en bloque y topics (Latencia Crucial)
    tags_info = []
    for equipo, variables in MARCAS.items():
        for var_name, (db, byte_off, bit_off, dtype) in variables.items():
            tags_info.append({
                'equipo': equipo,
                'db': db,
                'offset': byte_off,
                'bit': bit_off,
                'type': dtype,
                'amount': 4 if dtype == 'REAL' else 1,
                'topic': f"{MQTT_TOPIC_PREFIX}/{equipo}/{var_name}"
            })
    
    items_count = len(tags_info)
    data_items = (S7DataItem * items_count)()
    
    for i, info in enumerate(tags_info):
        info['buffer'] = (c_ubyte * info['amount'])()
        data_items[i].Area = ctypes.c_int32(Areas.DB.value)
        data_items[i].WordLen = ctypes.c_int32(WordLen.Byte.value)
        data_items[i].DBNumber = ctypes.c_int32(info['db'])
        data_items[i].Start = ctypes.c_int32(info['offset'])
        data_items[i].Amount = ctypes.c_int32(info['amount'])
        data_items[i].pData = ctypes.cast(ctypes.pointer(info['buffer']), POINTER(c_ubyte))
    
    logging.info("Sistema listo. Iniciando publicación con MQTT.")
    
    while running:
        try:
            if not plc.get_connected():
                plc = connect_plc()

            # Lectura multi-variable de Snap7 (Un solo viaje de red)
            ret_code, results = plc.read_multi_vars(data_items)
            
            batch = []
            # Generamos timestamp en milisegundos para exactitud en TSDB
            ts_ms = int(time.time() * 1000)
            
            for i, item in enumerate(results):
                if item.Result == 0:
                    info = tags_info[i]
                    data = bytearray(info['buffer'])
                    
                    if info['type'] == 'REAL':
                        valor = get_real(data, 0)
                    else:
                        valor = 1 if get_bool(data, 0, info['bit']) else 0
                    
                    batch.append((info['topic'], info['equipo'], valor, ts_ms))
            
            if batch:
                try:
                    # Usamos put_nowait para no bloquear el bucle de lectura si se cae MQTT
                    metrics_queue.put_nowait(batch)
                except queue.Full:
                    logging.warning("Cola MQTT llena. Descartando lote temporal para dar prioridad a la latencia.")
                
            time.sleep(SENSOR_READ_INTERVAL)
            
        except Exception as e:
            logging.error(f"Error en bucle: {e}")
            time.sleep(RETRY_DELAY)

    if plc.get_connected():
        plc.disconnect()
        logging.info("PLC Desconectado de forma segura.")

if __name__ == "__main__":
    main()
