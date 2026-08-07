import time
import logging
import signal
import pysparkplug as psp
from config import AppConfig
from plc_client import PLCClient
from mqtt_publisher import MQTTPublisher
from event_detector import EventDetector

logger = logging.getLogger("PLC-MQTT.IIoTGateway")

class IIoTGateway:
    """Orquestador principal del Edge Gateway IIoT."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.plc_client = PLCClient(config)
        self.mqtt_publisher = MQTTPublisher(config)
        self.event_detector = EventDetector(config.MARCAS)
        self.running = False
        
        # Registrar señales de apagado seguro
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, sig, frame):
        """Captura interrupciones de sistema para apagar el gateway ordenadamente."""
        logger.info("Interrupcion recibida. Apagando...")
        self.stop()

    def start(self):
        """Inicia el gateway y arranca el bucle de captura y transmisión."""
        logger.info("Iniciando componentes...")
        
        self.running = True
        
        # 1. Conectar al PLC de manera bloqueante/robusta, pero respetando señal de apagado
        if not self.plc_client.connect(lambda: self.running):
            logger.info("Arranque cancelado. Cerrando...")
            self._cleanup()
            return
        
        # 2. Arrancar el publicador asíncrono de MQTT
        self.mqtt_publisher.start()
        
        logger.info("Sistema listo. Iniciando captura.")
        
        # 3. Iniciar bucle principal de telemetría
        self._run_loop()

    def _run_loop(self):
        """Bucle continuo de lectura de variables y publicación."""
        
        consecutive_errors = 0
        max_errors = 5
        
        while self.running:
            try:
                # Verificar conectividad con el PLC antes de leer
                if not self.plc_client.is_connected():
                    logger.warning("PLC Desconectado. Reintentando conexión...")
                    if not self.plc_client.connect(lambda: self.running):
                        continue # Si retorna False, puede ser que se dio señal de apagado
                
                # Lectura multi-variable optimizada (un solo viaje de red)
                readings = self.plc_client.read_all_vars()
                
                updates_by_equipo = {}
                current_time = time.time()
                ts_ms = int(current_time * 1000)
                
                for info, valor in readings:
                    equipo = info['equipo']
                    var_name = info['var_name']
                    dtype = info['type']
                    last_val = info['last_value']
                    
                    should_publish = False
                    
                    if dtype == 'BOOL':
                        # Detección de cambio de estado
                        if last_val is None or last_val != valor:
                            should_publish = True
                    else: # REAL, WORD, INT, BYTE
                        # Detección por Banda Muerta (Deadband)
                        if last_val is None or abs(last_val - valor) >= info['deadband']:
                            should_publish = True
                            
                    # Evaluar si pasó el tiempo máximo de update (Sanity Check Period)
                    if not should_publish and (current_time - info['last_publish']) >= info['freq']:
                        should_publish = True
                    
                    # ── Evaluar eventos genéricos ──
                    events = self.event_detector.evaluate(equipo, var_name, valor)
                    for ev in events:
                        logger.info(ev.message)
                        ev_metric = psp.Metric(
                            timestamp=ts_ms,
                            name=ev.tag_name,
                            datatype=psp.DataType.INT32,
                            value=int(ev.value)
                        )
                        if ev.tag_equipo not in updates_by_equipo:
                            updates_by_equipo[ev.tag_equipo] = []
                        updates_by_equipo[ev.tag_equipo].append(ev_metric)
                    
                    if not should_publish:
                        continue
                        
                    # Actualizar memoria interna (estado y tiempo)
                    info['last_value'] = valor
                    info['last_publish'] = current_time
                    
                    # Mapear tipos y valores a Sparkplug B
                    if dtype in ('BOOL', 'INT', 'WORD', 'BYTE', 'DWORD', 'DINT'):
                        psp_dtype = psp.DataType.INT32
                        casted_val = int(valor)
                    else:
                        psp_dtype = psp.DataType.FLOAT
                        casted_val = float(valor)
                    
                    metric = psp.Metric(
                        timestamp=ts_ms,
                        name=var_name,
                        datatype=psp_dtype,
                        value=casted_val
                    )
                    
                    if equipo not in updates_by_equipo:
                        updates_by_equipo[equipo] = []
                    updates_by_equipo[equipo].append(metric)
                
                # Encolar las actualizaciones por dispositivo en el publicador asíncrono
                for equipo, metrics in updates_by_equipo.items():
                    self.mqtt_publisher.enqueue_device_update(equipo, metrics)
                
                # Tick rápido (0.1s) en lugar del viejo intervalo, para reaccionar rápido a las frecuencias
                time.sleep(0.1)
                consecutive_errors = 0  # Restablecemos errores al tener un ciclo exitoso
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error en bucle (Intento {consecutive_errors}/{max_errors}): {e}")
                if consecutive_errors >= max_errors:
                    logger.critical("Limite de errores. Abortando...")
                    raise RuntimeError("Abordaje de ejecución tras fallos persistentes.")
                time.sleep(self.config.retry_delay)

        self._cleanup()

    def stop(self):
        """Establece la señal de apagado del bucle principal."""
        self.running = False

    def _cleanup(self):
        """Libera de manera segura los recursos de red y comunicaciones."""
        logger.info("Cerrando recursos...")
        self.mqtt_publisher.stop()
        self.plc_client.disconnect()
        logger.info("Script detenido.")