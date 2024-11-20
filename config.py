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

OUTPUT_PATH = "output/datos_calidad_SINAC.csv"
OUTPUT_INVALID_PATH = "output/municipios_inválidos.csv"
SCRAPER_LOG = 'log/'

RETRIES = 4

