# streamlit_app.py
import streamlit as st
import asyncio
from data.scraper import SINACPayloadSraper, SINACRedScraper
from config import CA_NAMES, SCRAPER_LOG
import os

def initialize_session_state():
    """Initialize session state variables if they don't exist."""
    if 'data_df' not in st.session_state:
        st.session_state.data_df = None
    if 'invalid_mun_df' not in st.session_state:
        st.session_state.invalid_mun_df = None
    if 'progress' not in st.session_state:
        st.session_state.progress = 0
    if 'total_municipalities' not in st.session_state:
        st.session_state.total_municipalities = 0

def download_button(df, filename, button_text):
    """Create a download button for a dataframe."""
    if df is not None and not df.empty:
        csv = df.to_csv(index=False)
        st.download_button(
            label=button_text,
            data=csv,
            file_name=filename,
            mime='text/csv',
            key=filename  # Unique key to prevent button disappearance
        )

def main_app():
    st.title("Araña SINAC 🕷")

    # Initialize session state
    initialize_session_state()

    # Multiselect for Comunidades Autónomas
    com_names = st.multiselect(
        "Seleccione las comunidades autónomas a procesar:", 
        ["Todas"] + CA_NAMES,
        default=None
    )

    if not com_names:
        st.warning("¡Por favor, seleccione al menos una comunidad!")
        return

    # Determine community IDs
    if "Todas" in com_names:
        com_ids = list(range(1, len(CA_NAMES)+1))
    else:
        com_ids = [CA_NAMES.index(com) + 1 for com in com_names]

    # Scrape Data Button
    if st.button("Escrapear Datos"):
        # Create a progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        with st.spinner('Escrapeando datos...'):
            try:
                # First get the payloads (this will tell us total municipalities)
                payload_scraper = SINACPayloadSraper(com_ids)
                payload_df = asyncio.run(payload_scraper.scrape())
                
                # Initialize the total municipalities count
                total_municipalities = len(payload_df)
                st.session_state.total_municipalities = total_municipalities
                
                # Create data scraper with progress callback
                async def progress_callback(completed_municipalities):
                    progress = completed_municipalities / total_municipalities
                    progress_bar.progress(progress)
                    status_text.text(f"Hemos procesado {completed_municipalities} de {total_municipalities} municipios ({progress:.0%})")
                
                data_scraper = SINACRedScraper(payload_df, progress_callback=progress_callback)
                data_df, invalid_mun_df = asyncio.run(data_scraper.scrape())
                data_df.sort_values(by=['Comunidad Autónoma', 'Provincia', 'Municipio', 'Nombre de Red', 'Código']).reset_index(drop=True)
                invalid_mun_df.sort_values(by=['Comunidad Autónoma', 'Provincia', 'Municipio']).reset_index(drop=True)
                
                # Store results in session state
                st.session_state.data_df = data_df
                st.session_state.invalid_mun_df = invalid_mun_df

                # Display results
                st.success("¡Datos escrapeados con éxito!")
                st.write("Cantidad de filas escrapeadas:", len(data_df))
                st.write("Cantidad de municipios inválidos:", len(invalid_mun_df))

            except Exception as e:
                # create a download button for the error log
                st.write(e)
                st.write("Ha ocurrido un error. Por favor, descargue el log de errores y envíelo a Edu.")
                log_file = SCRAPER_LOG+f'scraper_{len(os.listdir(SCRAPER_LOG))-1}.log'
                with open(log_file, 'r') as f:
                    log = f.read()
                st.download_button(
                    label="Descargar Log de Errores",
                    data=log,
                    file_name="scraper_log.txt",
                    mime='text/plain',
                    key="error_log"
                )


    # Results Section (Always visible)
    st.header("Resultados")
    
    # Data DataFrame Display and Download
    if st.session_state.data_df is not None:
        st.subheader("Datos de Calidad de Agua")
        st.dataframe(st.session_state.data_df.head(9))
        download_button(
            st.session_state.data_df, 
            'datos_calidad_SINAC.csv', 
            'Descargar Datos de Calidad de Agua'
        )

    # Invalid Municipalities DataFrame Display and Download
    if st.session_state.invalid_mun_df is not None:
        st.subheader("Municipios Inválidos")
        st.dataframe(st.session_state.invalid_mun_df.head())
        download_button(
            st.session_state.invalid_mun_df, 
            'municipios_inválidos.csv', 
            'Descargar Municipios Inválidos'
        )

    if st.session_state.data_df is not None:
        st.subheader("Error Log")
        st.write("Para asegurar que no ha habido errores, por favor descargue el log de errores y envíelo a Edu.")
        log_file = SCRAPER_LOG+f'scraper_{len(os.listdir(SCRAPER_LOG))-1}.log'
        with open(log_file, 'r') as f:
            log = f.read()
        st.download_button(
            label="Descargar Log de Errores",
            data=log,
            file_name="scraper_log.txt",
            mime='text/plain',
            key="error_log"
        )

if __name__ == "__main__":
    main_app()