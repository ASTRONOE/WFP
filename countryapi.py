import logging
import pandas as pd
import requests
import geopandas as gpd
import os
from dotenv import load_dotenv
from io import StringIO
from deta import Deta
from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration
from hdx.facades.simple import facade
from hdx.data.dataset import Dataset

setup_logging()
logger  = logging.getLogger(__name__)

#initialize HDX configuration
#Configuration.create(hdx_site="prod", user_agent="DSWFProject", hdx_read_only=True)

df = pd.read_csv(r"C:\Users\USER\Documents\DSWFProject\WFP_Files\wfp_countries_global_1.csv")


load_dotenv()
key1 = os.getenv('DETA_PROJECT_KEY')
key2 = os.getenv('GEOJSON_KEY')


def read_data_from_hdx(data:str):
  """
  Retrieves information about food prices of a particular country from HDX.

  Parameters:
  data (str): A string value representing the data from the website 'https://https://data.humdata.org/dataset/'.

  Returns:
  An HDX object containing retrieved HDX information (if successful) and a dictionary containing specific data from the dataset related to the provided 'data'.
  """
  #attempt to read data in the wfp hdx via df
  assert data in df['directory'].values, f"Data '{data}' is not located inside the dataset"
  try:
      hdxinfo = Dataset.read_from_hdx(data) #read the data via the api
  except:
      logger.error("Could not get data or resource")
  else:
      logger.info(f"Getting data from 'https://https://data.humdata.org/dataset/{data}'")

  hdxinfo.set_expected_update_frequency('every week')
  
  #return the referenced data about the country
  return hdxinfo


def put_DB(data:str):   
  """
  Inserts data from the specified source into the provided database base.

  Parameters:
  - base: The database to which the data will be inserted.
  - data (str): The data read from the HDX.

  Operation:
  - Retrieves information from the specified data source and structures it into a dictionary.
  - Inserts the extracted information into the provided database base.

  Returns:
  None

  Raises:
  - If the data retrieval fails or the extracted information is empty, it raises an error.
  """
  hdxinfo = read_data(data)
  hdxres = hdxinfo.get_resources()
  hdxdic = {}
  hdxdic['Id'] = hdxinfo['id']
  hdxdic['Archived'] = str(hdxinfo['archived'])
  hdxdic['Country_id'] = hdxinfo['groups'][0]['id']
  hdxdic['Country_name'] = hdxinfo['groups'][0]['display_name'].upper()
  hdxdic['Name'] = hdxinfo['name']
  hdxdic['Due_date'] = hdxinfo['due_date']
  hdxdic['Overdue_date'] = hdxinfo['overdue_date']
  hdxdic['Resource_created'] = hdxres[0]['created']
  hdxdic['Download_URL'] = hdxres[0]['download_url']
  hdxdic['File_name'] = os.path.basename(hdxres[0]['download_url'])

  if hdxdic:
      dbase.put(data=hdxdic)
      logger.info(f'Included {data} inside database')
  else:
      logger.error(f'Could not insert {data} into database')

def worldmap():
  key = Deta(key2)
  dmap = key.Drive('GeoJSON')
  response = dmap.get('worldmap.json')
  content = response.read()
  decoded = content.decode('utf-8')
  decoded_df = StringIO(decoded)
  return gpd.read_file(decoded_df)
        
class CountryData:
  """
  Class representing a handler for to retreive WFP data about a specific country.

  Attributes:
  - base (deta.Base): The database instance.
  - dmap (deta.Drive): The map file storage.

  Methods:
  - get_DB(): Utilizes the GET method to retrieve data from the database using the key.
  - get_ref_period_DB(): Retrieves the reference period of a dataset from HDX.
  - get_data_from_link(): Fetches data from the dataset's download URL.
  - read_dataframe(): Reads the fetched data as a pandas DataFrame.
  - get_country_code(): Retrieves the country code from the database record.
  """
  def __init__(self, key:str):
    
    detas = Deta(key1)
    detam = Deta(key2)
    
    self.key = key #key to database record 
    self.base = detas.Base('WFPDatabase')
    self.maps = detam.Drive('GeoJSON')
    fetch = self.base.fetch().items
    assert self.key in [item['key'] for _, item in enumerate(fetch)], f"Not found. Unknown key" 
      
  def get_DB(self):
    """
    Utilises the GET method in order to get a data from the database using the key.
    Returns:
    A collection of information about the record as a dictionary
    """
    return self.base.get(self.key)  
  

  #yield content from the dataset's download url
  def __get_data_from_link(self):
    """
    Fetches data from the dataset's download URL.

    Yields:
    str: Content from the dataset's download URL in UTF-8 format.
    """
    item = self.get_DB()
    response = requests.get(url=item['Download_URL'], params={"downloadformat":"csv"})
    yield response.content.decode('utf-8')
      

  #after yielding the content from the url, generate the data
  #and read it as a pandas dataframe
  def read_dataframe(self):
    """
    Reads the fetched data as a pandas DataFrame.
    
    Returns:
    pandas.DataFrame: DataFrame containing the fetched data.
    """
    gen = self.__get_data_from_link()
    for con in gen:
        content = StringIO(con)
    df = pd.read_csv(content).drop(index=0)
    df = df.reset_index(drop=True)
    return df
  
  #get country code
  def get_country_code(self):
    """
    Reads the fetched data as a pandas DataFrame.
    
    Returns:
    pandas.DataFrame: DataFrame containing the fetched data.
    """
    item = self.get_DB()
    return item['Country_id']
  
  #Get dataset's reference period from hdx
  def get_ref_period(self):
    """
    Retrieves the reference period of a dataset from HDX.

    Returns:
    dict: The reference period of the dataset.
    """
    item = self.get_DB()
    read_item = Dataset.read_from_hdx(item['Name']) #get data from HDX
    read_item.set_expected_update_frequency('every week') #updated
    return read_item.get_reference_period()

  def __get_map(self):
    """Retrieve the map file content associated with the provided country code.

    Parameters:
    -----------
    country_code : str
        The country code to search for within the map file names.

    Returns:
    --------
    bytes or None
        The content of the map file corresponding to the provided country code.
        Returns None if no matching country code is found in the available map files.

    Raises:
    -------
    ValueError
        Raised when an invalid country code is provided and no corresponding file is found.
    """
    # Get list of json map files in the drive
    mapfiles = self.maps.list()['names']
    country_code = self.get_DB()['Country_id']


    # Iterate through the list of files
    for country_file in mapfiles:
      # Check if country is found, extract the content of the file using get()
        if country_code.upper() in country_file:
          get_country_file = self.maps.get(country_file).read()
          return get_country_file
    
  def read_map(self):
    """
    After getting the json shapefiles using __get_map(country_code), this function will decode the content as a string.
    Returns:
    -------
    geopandas.DataFrame
        a dataframe depicting the shapefile data.
    """
    #get map data
    country_file = self.__get_map()
    #decode data as a string
    decoded = country_file.decode('utf-8')
    map_df = StringIO(decoded)
    #geopandas dataframe
    return gpd.read_file(map_df)