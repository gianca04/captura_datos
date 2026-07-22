import time
import logging
import ctypes
from ctypes import POINTER, c_ubyte
import snap7
from snap7.util import get_real, get_bool, get_word, get_int, get_byte

# Resolver compatibilidad de snap7
try:
    from snap7.types import S7DataItem, Areas, WordLen
except ImportError:
    from snap7.snap7types import S7DataItem, Areas, WordLen

logger = logging.getLogger("PLC-MQTT.PLCClient")


def parse_area_and_db(area_input):
    """
    Parsea la indicación de área de memoria y número de DB.
    - int (ej: 65) -> DB 65
    - str ('I', 'PE', 'IW', 'ID', 'IB') -> Areas.PE (Inputs), db=0
    - str ('Q', 'PA', 'QW', 'QD', 'QB') -> Areas.PA (Outputs), db=0
    - str ('M', 'MK', 'MW', 'MD', 'MB') -> Areas.MK (Merker), db=0
    - str ('DB65' o '65') -> Areas.DB, db=65
    """
    if isinstance(area_input, int):
        return Areas.DB, area_input
    
    area_str = str(area_input).strip().upper()
    if area_str.startswith('DB'):
        db_num = int(area_str.replace('DB', '').strip())
        return Areas.DB, db_num
    elif area_str in ('I', 'PE', 'IW', 'ID', 'IB', 'INPUT'):
        return Areas.PE, 0
    elif area_str in ('Q', 'PA', 'QW', 'QD', 'QB', 'OUTPUT'):
        return Areas.PA, 0
    elif area_str in ('M', 'MK', 'MW', 'MD', 'MB', 'MERKER'):
        return Areas.MK, 0
    else:
        try:
            return Areas.DB, int(area_str)
        except ValueError:
            raise ValueError(f"Área PLC no válida: {area_input}")


def get_type_amount(dtype):
    dtype = str(dtype).upper()
    if dtype in ('REAL', 'DWORD', 'DINT'):
        return 4
    elif dtype in ('WORD', 'INT'):
        return 2
    elif dtype in ('BYTE', 'BOOL', 'CHAR'):
        return 1
    return 1


class PLCClient:
    """Clase OOP para gestionar la comunicación y lectura multi-variable con un PLC Siemens."""
    
    def __init__(self, config):
        self.config = config
        self.plc = snap7.client.Client()
        self.tags_info = []
        self.data_items = None
        self._initialize_tags_and_buffers()
        
    def _initialize_tags_and_buffers(self):
        """Pre-configura los buffers de memoria contiguos para optimizar la lectura por red."""
        for equipo, variables in self.config.MARCAS.items():
            for var_name, config_list in variables.items():
                area_or_db, byte_off, bit_off, dtype = config_list[:4]
                # Default a fall-back global interval si no existe en el JSON
                freq = config_list[4] if len(config_list) > 4 else self.config.sensor_read_interval
                deadband = config_list[5] if len(config_list) > 5 else 0.0
                
                area_enum, db_num = parse_area_and_db(area_or_db)
                dtype_str = str(dtype).upper()
                amount = get_type_amount(dtype_str)
                
                self.tags_info.append({
                    'equipo': equipo,
                    'var_name': var_name,
                    'area': area_enum,
                    'db': db_num,
                    'offset': byte_off,
                    'bit': bit_off,
                    'type': dtype_str,
                    'amount': amount,
                    'freq': freq,
                    'deadband': deadband,
                    'last_publish': 0.0, # Para llevar control del tiempo
                    'last_value': None   # Para control de Banda Muerta (RBE)
                })
        
        items_count = len(self.tags_info)
        # Crear estructura C de S7DataItem
        self.data_items = (S7DataItem * items_count)()
        
        for i, info in enumerate(self.tags_info):
            info['buffer'] = (c_ubyte * info['amount'])()
            self.data_items[i].Area = ctypes.c_int32(info['area'].value)
            self.data_items[i].WordLen = ctypes.c_int32(WordLen.Byte.value)
            self.data_items[i].DBNumber = ctypes.c_int32(info['db'])
            self.data_items[i].Start = ctypes.c_int32(info['offset'])
            self.data_items[i].Amount = ctypes.c_int32(info['amount'])
            self.data_items[i].pData = ctypes.cast(ctypes.pointer(info['buffer']), POINTER(c_ubyte))

    def connect(self, is_running_func=None) -> bool:
        """Establece una conexión robusta con reintentos detallados al PLC."""
        attempt = 1
        while is_running_func is None or is_running_func():
            try:
                logger.info(f"Conectando PLC {self.config.plc_ip} - Intento {attempt}")
                self.plc.connect(self.config.plc_ip, self.config.plc_rack, self.config.plc_slot)
                if self.plc.get_connected():
                    logger.info("PLC conectado.")
                    return True
            except Exception as e:
                err_msg = str(e).encode('ascii', 'ignore').decode('ascii')
                logger.error(f"Fallo conexion PLC: {err_msg}")
                try:
                    self.plc.disconnect()
                except Exception:
                    pass
            
            # Chequear si nos pidieron apagar mientras dormimos
            for _ in range(self.config.retry_delay):
                if is_running_func is not None and not is_running_func():
                    return False
                time.sleep(1)
            
            attempt += 1
            
        return False

    def is_connected(self) -> bool:
        """Verifica si la conexión actual del PLC sigue activa."""
        return self.plc.get_connected()

    def read_all_vars(self):
        """
        Ejecuta una lectura multi-variable optimizada.
        El protocolo S7 / Snap7 limita las lecturas múltiples a un máximo de 20 ítems por PDU.
        Divide automáticamente en lotes (chunks) de hasta 20 ítems.
        Retorna una lista de tuplas (info, valor) de las variables leídas exitosamente.
        """
        CHUNK_SIZE = 20
        items_count = len(self.tags_info)
        readings = []
        
        for start_idx in range(0, items_count, CHUNK_SIZE):
            end_idx = min(start_idx + CHUNK_SIZE, items_count)
            chunk_items = self.data_items[start_idx:end_idx]
            chunk_count = len(chunk_items)
            
            c_items = (S7DataItem * chunk_count)()
            for i, item in enumerate(chunk_items):
                c_items[i] = item
            
            ret_code, results = self.plc.read_multi_vars(c_items)
            
            for i, item in enumerate(results):
                tag_idx = start_idx + i
                info = self.tags_info[tag_idx]
                
                if item.Result == 0:
                    data = bytearray(info['buffer'])
                    dtype = info['type']
                    
                    if dtype == 'REAL':
                        valor = get_real(data, 0)
                    elif dtype == 'WORD':
                        valor = get_word(data, 0)
                    elif dtype == 'INT':
                        valor = get_int(data, 0)
                    elif dtype == 'BYTE':
                        valor = get_byte(data, 0)
                    elif dtype == 'BOOL':
                        valor = 1.0 if get_bool(data, 0, info['bit']) else 0.0
                    else:
                        valor = 0.0
                    
                    readings.append((info, valor))
                else:
                    logger.warning(f"Fallo lectura {info['equipo']}.{info['var_name']} (Res: {item.Result})")
                    
        return readings

    def disconnect(self):
        """Cierra la conexión de forma segura con el PLC."""
        if self.is_connected():
            self.plc.disconnect()
            logger.info("PLC desconectado.")

