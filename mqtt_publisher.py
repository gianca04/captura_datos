import time
import logging
import queue
import threading
import pysparkplug as psp
from event_detector import EventDetector

logger = logging.getLogger("PLC-MQTT.MQTTPublisher")

class MQTTPublisher:
    """Clase OOP que gestiona la conexión asíncrona a MQTT y la cola de mensajería (Worker) usando Sparkplug B."""
    
    def __init__(self, config):
        self.config = config
        self.metrics_queue = queue.Queue(maxsize=config.queue_maxsize)
        self.running = False
        self.client = None
        self.edge_node = None
        self.worker_thread = None
        self.event_detector = EventDetector(config.MARCAS)
        
        self._initialize_sparkplug_entities()

    def _initialize_sparkplug_entities(self):
        """Inicializa dinámicamente el cliente Sparkplug B, el Edge Node y los Dispositivos."""
        logger.info("Inicializando Sparkplug B...")
        
        # 1. Crear cliente MQTT subyacente
        self.client = psp.Client(
            client_id=f"{self.config.sparkplug_node_id}_client",
            username=self.config.mqtt_user,
            password=self.config.mqtt_password
        )
        
        # 2. Instanciar el Edge Node principal
        self.edge_node = psp.EdgeNode(
            group_id=self.config.sparkplug_group_id,
            edge_node_id=self.config.sparkplug_node_id,
            metrics=[],  # Sin métricas directamente en el nodo
            client=self.client
        )
        
        # 3. Registrar dispositivos dinámicamente basados en config.MARCAS
        for equipo, variables in self.config.MARCAS.items():
            device_metrics = []
            for var_name, config_list in variables.items():
                dtype = config_list[3]
                # Asignar tipos e inicializar valores de nacimiento (DBIRTH)
                if dtype in ('BOOL', 'WORD', 'INT', 'BYTE', 'DWORD', 'DINT'):
                    psp_dtype = psp.DataType.INT32
                    default_val = 0
                else:
                    psp_dtype = psp.DataType.FLOAT
                    default_val = 0.0
                
                metric = psp.Metric(
                    timestamp=int(time.time() * 1000),
                    name=var_name,
                    datatype=psp_dtype,
                    value=default_val
                )
                device_metrics.append(metric)
            
            # Crear métricas de evento virtuales vinculadas a este equipo
            for ev_tag in self.event_detector.get_event_tags():
                if ev_tag["tag_equipo"] == equipo:
                    ev_metric = psp.Metric(
                        timestamp=int(time.time() * 1000),
                        name=ev_tag["tag_name"],
                        datatype=psp.DataType.INT32,
                        value=0
                    )
                    device_metrics.append(ev_metric)
            
            # Crear y registrar el Device
            device = psp.Device(device_id=equipo, metrics=device_metrics)
            self.edge_node.register(device)
            logger.info(f"Registrado: {equipo}")

    def start(self):
        """Inicia el worker y gestiona la conexión asíncrona."""
        self.running = True
        self.worker_thread = threading.Thread(target=self._mqtt_worker, daemon=True)
        self.worker_thread.start()

    def _mqtt_worker(self):
        """Hilo consumidor: Se conecta al broker de forma asíncrona y despacha la cola de datos."""
        logger.info(f"Iniciando Worker MQTT -> {self.config.mqtt_broker}:{self.config.mqtt_port}")
        
        # Intentar conectar con reintentos
        while self.running:
            try:
                # Conectar el Edge Node de forma asíncrona (blocking=False)
                # Esto inicia el loop del cliente paho-mqtt en segundo plano
                self.edge_node.connect(self.config.mqtt_broker, port=self.config.mqtt_port, blocking=False)
                break
            except Exception as e:
                logger.error(f"Error conexion MQTT: {e}")
                time.sleep(self.config.retry_delay)

        # Esperar hasta que el Edge Node Sparkplug B esté completamente operativo
        # (NBIRTH + DBIRTH publicados exitosamente, no solo conexión TCP)
        logger.info("Esperando conexion MQTT...")
        while self.running and not self.edge_node._connected:
            time.sleep(0.1)
            
        if self.running:
            logger.info("Conexion MQTT establecida.")
            # Breve pausa para asegurar que los mensajes BIRTH sean procesados por el broker
            time.sleep(0.5)

        # Bucle principal de despacho de datos
        while self.running:
            try:
                # Si se pierde la conexión, pausar el despacho de la cola para no descartar/fallar
                if not self.edge_node._connected:
                    logger.warning("Conexion perdida con el Broker MQTT. Pausando despacho de telemetria...")
                    while self.running and not self.edge_node._connected:
                        time.sleep(0.5)
                    if not self.running:
                        break
                    logger.info("Conexion restablecida con el Broker MQTT. Reanudando despacho.")

                item = self.metrics_queue.get(timeout=1.0)
                if item is None:
                    break
                
                equipo, metrics = item
                # update_device construye el payload DDATA y lo publica de forma segura
                self.edge_node.update_device(equipo, metrics)
                self.metrics_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error en publicacion MQTT: {e}")

        # Desconexión segura y limpia
        logger.info("Deteniendo Worker MQTT...")
        if self.edge_node:
            try:
                # publicará NDEATH y desconectará
                self.edge_node.disconnect()
                logger.info("Nodo Sparkplug desconectado.")
            except Exception as e:
                logger.error(f"Error al desconectar Edge Node: {e}")

    def enqueue_device_update(self, equipo: str, metrics: list) -> bool:
        """Encola la actualización de métricas de un dispositivo para transmisión asíncrona."""
        if not self.running:
            logger.warning("Publicador MQTT detenido. Descartando datos.")
            return False
            
        try:
            self.metrics_queue.put_nowait((equipo, metrics))
            return True
        except queue.Full:
            logger.warning("Cola MQTT llena. Descartando datos.")
            return False

    def stop(self):
        """Detiene el worker y libera los recursos de Sparkplug."""
        self.running = False
        if self.worker_thread:
            try:
                self.metrics_queue.put_nowait(None)
            except queue.Full:
                pass
            self.worker_thread.join(timeout=3.0)
            logger.info("Worker MQTT finalizado.")
