import os
import sys
import time
import logging
import paho.mqtt.client as mqtt
import pysparkplug as psp
from config import AppConfig

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        topic_filter = "spBv1.0/#"
        logging.info(f"Conectado exitosamente al Broker MQTT ({userdata['broker']}). Suscribiendo a '{topic_filter}'...")
        client.subscribe(topic_filter)
    else:
        logging.error(f"Fallo al conectar al broker. Código de retorno: {rc}")

def on_message(client, userdata, msg):
    try:
        topic_str = msg.topic
        spb_msg = psp.Message.from_mqtt_message(msg)
        
        topic_obj = spb_msg.topic
        msg_type = topic_obj.message_type.name if hasattr(topic_obj.message_type, 'name') else str(topic_obj.message_type)
        group_id = getattr(topic_obj, 'group_id', '-')
        node_id = getattr(topic_obj, 'edge_node_id', '-')
        device_id = getattr(topic_obj, 'device_id', None)
        
        payload_obj = spb_msg.payload
        metrics = getattr(payload_obj, 'metrics', [])
        
        print("\n" + "="*70)
        print(f"[MENSAJE RECIBIDO] Topic: {topic_str}")
        print(f"   Group ID     : {group_id}")
        print(f"   Node ID      : {node_id}")
        if device_id:
            print(f"   Device ID    : {device_id}")
        print(f"   Tipo Mensaje : {msg_type}")
        print("-"*70)
        
        if metrics:
            print(f"   Metricas ({len(metrics)}):")
            for m in metrics:
                dt_str = m.datatype.name if hasattr(m.datatype, 'name') else str(m.datatype)
                print(f"    * {m.name}: {m.value} (Tipo: {dt_str})")
        else:
            print("   Metricas: (sin metricas en este payload)")
        print("="*70)
        
    except Exception as e:
        print(f"\n[Sparkplug Decoder Error] Error en {msg.topic}: {e}")

def main():
    config = AppConfig()
    print("="*70)
    print("INICIANDO SUSCRIPTOR DE PRUEBA SPARKPLUG B")
    print(f"   Broker   : {config.mqtt_broker}:{config.mqtt_port}")
    print(f"   Usuario  : {config.mqtt_user}")
    print(f"   Topic    : spBv1.0/#")
    print("="*70)

    client = mqtt.Client(client_id="Test_Subscriber_SparkplugB", userdata={"broker": config.mqtt_broker})
    if config.mqtt_user and config.mqtt_password:
        client.username_pw_set(config.mqtt_user, config.mqtt_password)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(config.mqtt_broker, config.mqtt_port, 60)
        client.loop_start()
        
        print("\nEscuchando mensajes Sparkplug B durante 10 segundos...\n")
        time.sleep(10)
        
    except KeyboardInterrupt:
        print("\nDeteniendo suscriptor de prueba...")
    except Exception as e:
        logging.error(f"Error en la conexion MQTT: {e}")
    finally:
        client.loop_stop()
        client.disconnect()
        print("Conexion cerrada.")

if __name__ == "__main__":
    main()