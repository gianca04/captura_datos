import time
import sys
import logging
from datetime import datetime
from config import AppConfig
from plc_client import PLCClient

# Configurar encoding UTF-8 en stdout si es Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Configurar logging estructurado para consola
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("PLC-Monitor")


def format_value(dtype, value):
    """Formatea el valor para despliegue visual claro en consola."""
    if dtype == 'BOOL':
        is_on = bool(value)
        return "[ON] (1)" if is_on else "[OFF] (0)"
    elif dtype == 'REAL':
        return f"{value:.2f}"
    elif dtype in ('WORD', 'INT', 'BYTE'):
        val_int = int(value)
        return f"{val_int} (0x{val_int:04X})" if dtype == 'WORD' else f"{val_int}"
    return str(value)


def main():
    print("=" * 85)
    print("      MONITOR DE ADQUISICION DE DATOS EN TIEMPO REAL - PLC SIEMENS (SIN MQTT)")
    print("=" * 85)
    
    config = AppConfig()
    client = PLCClient(config)
    
    logger.info(f"Cargados {len(client.tags_info)} tags desde tags_plc.json")
    logger.info(f"Conectando al PLC Siemens {config.plc_ip} (Rack {config.plc_rack}, Slot {config.plc_slot})...")
    
    if not client.connect():
        logger.error("No se pudo conectar con el PLC Siemens. Abortando.")
        return
        
    logger.info("Conexion establecida. Iniciando adquisicion continua de datos...\n")
    
    cycle = 1
    try:
        while True:
            timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            readings = client.read_all_vars()
            
            active_components = []
            grouped = {}
            
            for info, valor in readings:
                equipo = info['equipo']
                var_name = info['var_name']
                dtype = info['type']
                
                if equipo not in grouped:
                    grouped[equipo] = []
                
                formatted_val = format_value(dtype, valor)
                grouped[equipo].append((var_name, dtype, info['area'].name, info['offset'], info['bit'], valor, formatted_val))
                
                # Identificación de proceso encendido / activo
                if dtype == 'BOOL' and bool(valor):
                    active_components.append(f"{equipo}.{var_name} = ON")
                elif dtype in ('REAL', 'WORD', 'INT') and valor not in (0, 0.0, 32768, 65535, 65340, 65341):
                    # Excluir valores de desbordamiento/abierto en entradas no cableadas
                    active_components.append(f"{equipo}.{var_name} = {valor}")

            # Imprimir encabezado de ciclo
            print(f"\n==========================================")
            print(f" [CICLO #{cycle}] - {timestamp_str}")
            print(f"==========================================")
            
            if active_components:
                print(">> ESTADO DEL PROCESO: SEÑALES Y COMPONENTES ACTIVOS DETECTADOS:")
                for act in active_components:
                    print(f"   * {act}")
            else:
                print(">> ESTADO DEL PROCESO: EN REPOSO / DETENIDO (Sin señales de encendido activas)")
            
            print("\nDETALLE DE ADQUISICION DE TAGS EN TIEMPO REAL:")
            print("-" * 85)
            print(f"{'EQUIPO':<15} | {'VARIABLE':<18} | {'AREA':<6} | {'TIPO':<5} | {'VALOR REGISTRADO':<25}")
            print("-" * 85)
            
            for equipo, vars_list in grouped.items():
                for var_name, dtype, area_name, offset, bit, raw_val, formatted_val in vars_list:
                    print(f"{equipo:<15} | {var_name:<18} | {area_name:<6} | {dtype:<5} | {formatted_val:<25}")
            
            print("-" * 85)
            
            cycle += 1
            time.sleep(2.0)
            
    except KeyboardInterrupt:
        print("\n\nMonitoreo detenido por el usuario.")
    finally:
        client.disconnect()
        logger.info("Conexion con el PLC liberada correctamente.")

if __name__ == "__main__":
    main()
