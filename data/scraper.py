import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Tuple, Dict, Any, Optional
import logging
from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryCallState
from tqdm.asyncio import tqdm as async_tqdm
import pickle
import os

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from config import CA_NAMES, PROV_URL, MUN_URL, PAYLOAD_PATH, NET_URL, RED_PAYLOAD_PATH, \
    RED_PATH, INVALID_PATH, CONTENT_URL, PARAMETER_CODES, SCRAPER_LOG, RETRIES
from utils.ratelimiter import RateLimiter

class RequestError(Exception):
    """Base class for request-related errors."""
    pass

class TooManyRequestsError(RequestError):
    """Raised when receiving a 429 Too Many Requests response."""
    pass

class NetworkError(RequestError):
    """Raised for network-related errors."""
    pass

class ServerError(RequestError):
    """Raised for 5xx server errors."""
    pass

def log_failure(retry_state: RetryCallState):
    if retry_state.attempt_number == RETRIES:    
        scraper_obj = retry_state.args[0]
        scraper_obj.logger.error(f"Failed to fetch data after {retry_state.attempt_number} attempts.")
        payload = retry_state.args[3] 
        scraper_obj.logger.error(f"Payload: {payload}")


class SINACPayloadSraper:
    def __init__(self, CA_codes: List[int]):
        self.base_urls = {
            'provinces': PROV_URL,
            'municipalities': MUN_URL
        }
        
        self.CA_codes = CA_codes
        self.communities = self._initialize_communities()
        self.results_df = pd.DataFrame(
            columns=['community_name', 'province', 'municipe', 'payload']
        )
        self.save_path = PAYLOAD_PATH
        log_file = SCRAPER_LOG+f'scraper_{len(os.listdir(SCRAPER_LOG))}.log'
        # Configure logging
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logger file: " + log_file)
        self.logger.info("Starting the payload generation:\n")

        self.timeout = ClientTimeout(total=30, connect=10, sock_read=10)
        self.df_lock = asyncio.Lock()

    async def _save_results(self):
        async with self.df_lock:
            pickle.dump(self.results_df, open(self.save_path, 'wb'))

    def _initialize_communities(self) -> List[Tuple[str, str]]:
        """Initialize the list of Spanish autonomous communities."""
        CA_names = [CA_NAMES[code-1] for code in self.CA_codes]
        return list(zip(self.CA_codes, CA_names))

    @staticmethod
    def _define_payload(cod_comunidad: str, cod_provincia: str, cod_municipio: str) -> Dict[str, str]:
        """Create the payload for the final search request."""
        return {
            'codComunidad': cod_comunidad,
            'codProvincia': cod_provincia,
            'codMunicipio': cod_municipio,
            'method': 'Buscar'
        }

    @staticmethod
    def _parse_options(html_content: str) -> List[Tuple[str, str]]:
        """Parse HTML content and extract options with their values."""
        soup = BeautifulSoup(html_content, 'html.parser')
        options = soup.find_all('option')
        return [(option['value'], option.text) for option in options if option['value'] != '']

    async def _handle_response(self, response: aiohttp.ClientResponse) -> str:
        """Handle the response and raise appropriate exceptions."""
        if response.status == 429:
            raise TooManyRequestsError(f"Too many requests: {response.status}")
        elif response.status >= 500:
            raise ServerError(f"Server error: {response.status}")
        elif response.status >= 400:
            raise RequestError(f"Request error: {response.status}")
            
        try:
            return await response.text()
        except Exception as e:
            raise NetworkError(f"Error reading response: {str(e)}")

    @retry(
        stop=stop_after_attempt(RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((TooManyRequestsError, ServerError, NetworkError)),
        before_sleep=lambda retry_state: logging.info(
            f"Retrying in {retry_state.next_action.sleep} seconds..."
        )
    )
    async def _fetch_data(
        self,
        session: aiohttp.ClientSession,
        url: str,
        payload: Dict[str, str],
        timeout: Optional[ClientTimeout] = None
    ) -> str:
        """
        Fetch data from the specified URL with the given payload.
        Includes rate limiting and error handling.
        """
        # await self.rate_limiter.acquire()
        
        try:
            async with session.post(url, data=payload, timeout=timeout or self.timeout) as response:
                return await self._handle_response(response)
                
        except asyncio.TimeoutError as e:
            self.logger.error(f"Timeout error for {url}: {str(e)}")
            raise NetworkError(f"Request timed out: {str(e)}")
        except aiohttp.ClientError as e:
            self.logger.error(f"Client error for {url}: {str(e)}")
            raise NetworkError(f"Network error: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error fetching data from {url}: {str(e)}")
            raise

    async def _process_municipality(
        self,
        community_name: str,
        community_id: str,
        province_name: str,
        province_id: str,
        municipality_id: str,
        municipality_name: str
    ):
        """Process a single municipality and store its data."""
        try:
            final_payload = self._define_payload(community_id, province_id, municipality_id)
            new_row = pd.DataFrame([{
                'community_name': community_name,
                'province': province_name,
                'municipe': municipality_name,
                'payload': final_payload
            }])
            async with self.df_lock:
                self.results_df = pd.concat([self.results_df, new_row], ignore_index=True)
        except Exception as e:
            self.logger.error(
                f"Error processing municipality {municipality_name} in {province_name}: {str(e)}"
            )
            raise

    async def _process_province(
        self,
        session: aiohttp.ClientSession,
        community_name: str,
        community_id: str,
        province_name: str,
        province_id: str
    ):
        """Process all municipalities in a province."""
        try:
            html_content = await self._fetch_data(
                session,
                self.base_urls['municipalities'],
                {'id': province_id}
            )
            municipalities = self._parse_options(html_content)
            
            await asyncio.gather(*[
                self._process_municipality(
                    community_name, community_id,
                    province_name, province_id,
                    muni_id, muni_name
                )
                for muni_id, muni_name in municipalities
            ])
        except Exception as e:
            self.logger.error(f"Error processing province {province_name}: {str(e)}")
            raise

    async def _process_community(
        self,
        session: aiohttp.ClientSession,
        community_id: str,
        community_name: str
    ):
        """Process all provinces in a community."""
        try:
            self.logger.info(f"Processing community: {community_name}")
            html_content = await self._fetch_data(
                session,
                self.base_urls['provinces'],
                {'id': community_id}
            )
            provinces = self._parse_options(html_content)
            
            await asyncio.gather(*[
                self._process_province(
                    session, community_name, community_id,
                    province_name, province_id
                )
                for province_id, province_name in provinces
            ])
            await self._save_results()
        except Exception as e:
            self.logger.error(f"Error processing community {community_name}: {str(e)}")
            raise

    async def scrape(self) -> pd.DataFrame:
        """Main method to scrape all data."""
        timeout = ClientTimeout(total=300)  # 5 minutes total timeout for the entire session
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                await asyncio.gather(*[
                    self._process_community(session, comm_id, comm_name)
                    for comm_id, comm_name in async_tqdm(self.communities)
                ])
                return self.results_df
            except Exception as e:
                self.logger.error(f"Error during scraping: {str(e)}")
                raise

    def run(self) -> pd.DataFrame:
        """Convenience method to run the scraper."""
        return asyncio.run(self.scrape())
    


class SINACRedScraper:
    def __init__(self, payload_df: pd.DataFrame = None, calls_per_second: int = 30, progress_callback=None):
        self.red_url = NET_URL
        self.content_url = CONTENT_URL
        self.payload_path = PAYLOAD_PATH
        self.mun_payloads, self.munid2names = self._initialize_mun_payloads(payload_df)
        self.progress_callback = progress_callback
        self.processed_municipalities = 0
        
        red_payload_df = pd.DataFrame(
            columns=['community_name', 'province', 'municipe', 'red_id', 'red_name', 'payload']
        )
        invalid_mun_df = pd.DataFrame(
            columns=['Comunidad Autónoma', 'Provincia', 'Municipio']
        )
        red_data_df = pd.DataFrame(
            columns=['Comunidad Autónoma', 'Provincia', 'Municipio', 'Nombre de Red', 
                     'Código', 'Parámetro', 'Valor', 'Unidad', 'Fecha']
        )
        self.dataframes = {
            'payload': red_payload_df,
            'invalid': invalid_mun_df,
            'red': red_data_df
        }

        self.save_paths = {
            'payload': RED_PAYLOAD_PATH,
            'invalid': INVALID_PATH,
            'red': RED_PATH
        }

        # Initialize rate limiter
        self.rate_limiter = RateLimiter(calls_per_second)
        
        log_file = SCRAPER_LOG + f'scraper_{len(os.listdir(SCRAPER_LOG))-1}.log'
        # Configure logging
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Starting the red data extraction:\n")

        self.timeout = ClientTimeout(total=180, connect=90, sock_read=90)
        self.df_locks = {
            'payload': asyncio.Lock(),
            'invalid': asyncio.Lock(),
            'red': asyncio.Lock()
        }

    async def _save_results(self, df_name: str):
        async with self.df_locks[df_name]:
            pickle.dump(self.dataframes[df_name], open(self.save_paths[df_name], 'wb'))

    def _initialize_mun_payloads(self, payload_df: pd.DataFrame= None) -> Tuple[List, Dict[str, Dict[str, str]]]:
        if payload_df is None:
            payload_df = pickle.load(open(self.payload_path, 'rb'))
        return payload_df['payload'].to_list(), payload_df[['community_name', 'province', 'municipe']].to_dict(orient='records')
    
    async def _update_progress(self):
        if self.progress_callback:
            self.processed_municipalities += 1
            await self.progress_callback(self.processed_municipalities)

    @staticmethod
    def _define_payload(cod_comunidad: str, cod_provincia: str, cod_municipio: str, cod_red: str) -> Dict[str, str]:
        """Create the payload for the final search request."""
        return {
            'codComunidad': cod_comunidad,
            'codProvincia': cod_provincia,
            'codMunicipio': cod_municipio,
            'idRed': cod_red
        }
    
    @staticmethod
    def _parse_table(html_content: str) -> List[Tuple[str, str]]:
        """Parse HTML content and extract options with their values."""
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', id='red')
        if not table:
            return []
        redes = []
        for row in table.find_all('tr'):
            link = row.find('a')
            if link:
                red_code = link['href'].split('eleccionRedDistribucion(')[1].split(')')[0]
                red_name = link.text.strip()
                redes.append((red_code, red_name))
        return redes
    
    @staticmethod
    def _parse_data(html_content: str) -> pd.DataFrame:
        start_idx = html_content.find('Últimos valor notificado de los parámetros de la legislación vigente')
        start_id_table = html_content[start_idx:].find('<table')
        end_id_table = html_content[start_idx:].find('</table')

        html_table = html_content[start_idx+start_id_table:start_idx+end_id_table] + '</table>' 
        soup_table = BeautifulSoup(html_table, 'html.parser')

        table = soup_table.find('table')
        df = pd.read_html(str(table))[0]
        df = df[df['Código'].isin(PARAMETER_CODES)]
        df.rename(columns={'Valor cuantificado': 'Valor'}, inplace=True)
        return df

    async def _handle_response(self, response: aiohttp.ClientResponse, payload: Dict[str, str]) -> str:
        """Handle the response and raise appropriate exceptions."""
        if response.status == 429:
            raise TooManyRequestsError(f"Too many requests: {response.status}")
        elif response.status == 404:
            self.logger.error(f"Requesting URL with payload: {payload}")
        elif response.status >= 500:
            raise ServerError(f"Server error: {response.status}")
        elif response.status >= 400:
            raise RequestError(f"Request error: {response.status}")
            
        try:
            return await response.text()
        except Exception as e:
            raise NetworkError(f"Error reading response: {str(e)}")

    @retry(
        stop=stop_after_attempt(RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((TooManyRequestsError, ServerError, RequestError, NetworkError)),
        before_sleep=lambda retry_state: logging.info(
            f"Retrying in {retry_state.next_action.sleep} seconds...",
        ),
        after=log_failure
    )
    async def _fetch_data(
        self,
        session: aiohttp.ClientSession,
        url: str,
        payload: Dict[str, str],
        timeout: Optional[ClientTimeout] = None
    ) -> str:
        """
        Fetch data from the specified URL with the given payload.
        Includes rate limiting and error handling.
        """
        await self.rate_limiter.acquire()
        
        try:
            async with session.post(url, data=payload, timeout=timeout or self.timeout) as response:
                return await self._handle_response(response, payload)
                
        except asyncio.TimeoutError as e:
            self.logger.error(f"Timeout error for {url}: {str(e)}")
            raise NetworkError(f"Request timed out: {str(e)}")
        except aiohttp.ClientError as e:
            self.logger.error(f"Client error for {url}: {str(e)}")
            raise NetworkError(f"Network error: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error fetching data from {url}: {str(e)}")
            raise

    async def _process_red(
        self,
        session: aiohttp.ClientSession,
        community_name: str,
        community_id: str,
        province_name: str,
        province_id: str,
        municipe_name: str,
        municipe_id: str,
        red_name: str,
        red_id: str
    ):
        """Process a single red and store its data."""
        try:
            red_payload = self._define_payload(community_id, province_id, municipe_id, red_id)
            html_content = await self._fetch_data(
                session,
                self.content_url,
                red_payload
            )
            red_data = self._parse_data(html_content)
            if not red_data.empty:
                red_data['Comunidad Autónoma'] = community_name
                red_data['Provincia'] = province_name
                red_data['Municipio'] = municipe_name
                red_data['Nombre de Red'] = red_name
                async with self.df_locks['red']:
                    self.dataframes['red'] = pd.concat([self.dataframes['red'], red_data], ignore_index=True)

        except Exception as e:
            self.logger.error(
                f"Error processing red {red_name} in {municipe_name}: {str(e)}"
            )
            raise

    async def _process_mun_reds(
        self,
        session: aiohttp.ClientSession,
        mun_id: str,
        payload: Dict[str, str]
    ):
        """Process all reds in a municipality."""
        try:
            html_content = await self._fetch_data(
                session,
                self.red_url,
                payload
            )
            reds = self._parse_table(html_content)

            if len(reds) == 0:
                async with self.df_locks['invalid']:
                    values = list(self.munid2names[mun_id].values())

                    self.dataframes['invalid'] = pd.concat(
                        [self.dataframes['invalid'], pd.DataFrame(values, index=self.dataframes['invalid'].columns).T],
                        ignore_index=True
                    )
                await self._update_progress()
                return

            
            await asyncio.gather(*[
                self._process_red(
                    session, self.munid2names[mun_id]['community_name'], payload['codComunidad'],
                    self.munid2names[mun_id]['province'], payload['codProvincia'], 
                    self.munid2names[mun_id]['municipe'], payload['codMunicipio'],
                    red_name, red_id)
                for red_id, red_name in reds
            ])
            await asyncio.gather(*[self._save_results(df_name) for df_name in self.dataframes.keys()])

            await self._update_progress()
        except Exception as e:
            self.logger.error(f"Error processing municipality {mun_id}: {str(e)}")
            raise

    async def scrape(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        timeout = ClientTimeout(total=300)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                # Create tasks
                tasks = [
                    self._process_mun_reds(session, mun_id, payload)
                    for mun_id, payload in enumerate(self.mun_payloads)
                ]
                
                # Use tqdm with asyncio.as_completed() for progress tracking
                for f in async_tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Extracting data"):
                    await f
                
                return self.dataframes['red'], self.dataframes['invalid']
            except Exception as e:
                self.logger.error(f"Error during scraping: {str(e)}")
                raise

    def run(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Convenience method to run the scraper."""
        return asyncio.run(self.scrape())