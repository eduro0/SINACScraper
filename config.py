CA_NAMES = ['Andalucía', 'Aragón', 'Asturias', 'Baleares', 'Canarias', 'Cantabria', 'Castilla y León', 'Castilla-La Mancha', 'Cataluña', 'Comunidad Valenciana', 'Extremadura', 'Galicia', 'Madrid', 'Murcia', 'Navarra', 'País Vasco', 'La Rioja', 'Ceuta', 'Melilla'] # The names of the communities
PARAMETER_CODES = {26, 46, 47, 51, 52, 53, 64, 65, 66, 67}

PROV_URL = "https://sinac.sanidad.gob.es/CiudadanoWeb/ciudadano/cargarComboProvinciasAction.do"
MUN_URL = "https://sinac.sanidad.gob.es/CiudadanoWeb/ciudadano/cargarComboMunicipiosAction.do"
NET_URL = 'https://sinac.sanidad.gob.es/CiudadanoWeb/ciudadano/informacionRedes.do'
CONTENT_URL = 'https://sinac.sanidad.gob.es/CiudadanoWeb/ciudadano/informacionAbastecimientoActionDetalleRed.do'

PAYLOAD_PATH = "data/payloads.pkl"
RED_PAYLOAD_PATH = "data/red_payloads.pkl"
RED_PATH = "data/red_data.pkl"
INVALID_PATH = "data/invalid_municipalities.pkl"

# output as excel files
OUTPUT_PATH = "output/datos_calidad_SINAC.xlsx"
OUTPUT_INVALID_PATH = "output/municipios_inválidos.xlsx"
SCRAPER_LOG = 'log/'

WATER_DATA_FILENAME = 'datos_calidad_SINAC.xlsx'
INVALID_MUN_FILENAME = 'municipios_inválidos.xlsx'
LOG_FILENAME = 'scraper_log.txt'

RETRIES = 4

CORRECT_PASSWORD = 'a946ba1542381a93b8c41de17e586d69c2382637fbb46451a812715d82ad5d59'

