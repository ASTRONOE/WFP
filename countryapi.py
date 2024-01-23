import logging
import pandas as pd
import datatable as dt
from datatable import f
import requests
import geopandas as gpd
import os
from dotenv import load_dotenv
from io import StringIO
from deta import Deta
# hdx.data.dataset import Dataset
# from hdx.utilities.easy_logging import setup_logging
# from hdx.api.configuration import Configuration
# from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

#initialize HDX configuration
#setup_logging()
#Configuration.create(hdx_site="prod", user_agent="DSWFProject", hdx_read_only=True)

load_dotenv()
key1 = os.environ.get('DETA_PROJECT_KEY')
key2 = os.environ.get('GEOJSON_KEY')

wfp = pd.read_csv('assets/wfp_countries_global_1.csv')
world = gpd.read_feather('assets/worldmap.feather', columns=['sovereignt', 'sov_a3', 'level', 'adm0_iso', 'admin', 'name', 'name_long', 'brk_a3', 'brk_name', 'abbrev', 'geometry'])


def read_data_from_hdx(data: str):
  """
  Retrieves information about food prices of a particular country from HDX.

  Parameters:
  data (str): A string value representing the data from the website 'https://https://data.humdata.org/dataset/'.

  Returns:
  An HDX object containing retrieved HDX information (if successful) and a dictionary containing specific data from the dataset related to the provided 'data'.
  """
  #attempt to read data in the wfp hdx via wpf
  hdxinfo = None
  assert data in wfp['directory'].values, f"Data '{data}' is not located inside the dataset"

  try:
    hdxinfo = Dataset.read_from_hdx(data)  #read the data via the api
  except Exception as e:
    logger.error(f"Could not get data or resource {e}")
  else:
    logger.info(f"Getting data from 'https://https://data.humdata.org/dataset/{data}'")

  #hdxinfo.set_expected_update_frequency('every week')

  #return the referenced data about the country
  return hdxinfo


def put_DB(data: str, base):
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
  hdxinfo = read_data_from_hdx(data)
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
    base.put(data=hdxdic)
    logger.info(f'Included {data} inside database')
  else:
    logger.error(f'Could not insert {data} into database')


class CountryData:

  def __init__(self, key: str):
    #open the database
    detas = Deta(key1)
    detam = Deta(key2)
    #access the content of the wfp data and shapefiles
    self.__key = key  #key to database record
    self.__base = detas.Base('WFPDatabase')
    self.__maps = detam.Drive('GeoJSON')
    fetch = self.__base.fetch().items
    #ensure that the key input is valid
    assert key in [item['key']
                   for _, item in enumerate(fetch)], f"Not found. Unknown key"

  def __get_DB(self):
    """
    Utilises the GET method in order to get a data from the database using the key.
    Returns:
    A collection of information about the record as a dictionary
    """
    key = self.__key
    return self.__base.get(key)

  #private class method to get data from different countries
  @classmethod
  def __get_some_DB(cls, iso_request):
    """
    Retrieves data from the WFP database based on a list of ISO codes.

    Parameters:
    iso_request (list): A list of ISO codes for the requested countries.

    Returns:
    list: A list of dictionaries containing data for the requested countries from the database.
    """
    #open database and access all items
    detas = Deta(key1)
    base = detas.Base('WFPDatabase')
    fetch = base.fetch().items
    #convert requested list into set
    iso = set(iso_request)
    #get set of country ISOs in the database
    isolist = set([item['Country_id'] for _, item in enumerate(fetch)])
    #ensure that all requested ISOs are also inside the database
    country_iso = iso & isolist
    #fetch data of the requested ISOs
    #data = []
    for name in list(country_iso):
      yield from (item for item in fetch if item['Country_id'] == name)

    #   data.extend([item for item in fetch if item['Country_id'] == name])
    # return data

  #static method to return data as objects
  @classmethod
  def get_some_countries(cls, iso):
    """
    Retrieves CountryData objects for specified ISO codes from the WFP database.

    Parameters:
    iso (list): A list of ISO codes for which CountryData objects are requested.

    Returns:
    list: A list of CountryData objects representing the specified countries.
    """
    #get requested data
    data = cls.__get_some_DB(iso)
    dict_wfp = {}
    for d in data:
      #represent everything as an object of CountryData
      country_obj = CountryData(d['key'])
      dict_wfp[d['Country_id']] = country_obj
    #put everything into a list of objects
    return dict_wfp

  #yield content from the dataset's download url
  def __get_data_from_link(self):
    """
    Fetches data from the dataset's download URL.

    Yields:
    str: Content from the dataset's download URL in UTF-8 format.
    """
    item = self.__get_DB()
    response = requests.get(url=item['Download_URL'], params={"downloadformat": "csv"})
    yield response.content.decode('utf-8')

  #after yielding the content from the url, generate the data and read it as a pandas dataframe

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

  def read_dataframe(self):
    """
    Reads the fetched data as a pandas DataFrame.
    Returns:
    pandas.DataFrame: DataFrame containing the fetched data.
    """
    gen = self.__get_data_from_link()
    for con in gen:
      content = StringIO(con)
    df = pd.read_csv(content).drop(index=0)  #remove description
    df = df.reset_index(drop=True)  #reset index
    return df

  def read_datatable(self):
    """
    Reads the fetched data as a datatable.

    Returns:
    datatable containing the fetched data.
    """
    gen = self.__get_data_from_link()
    for con in gen:
      content = StringIO(con)
    Dt = dt.fread(content)
    return Dt

  #get country code
  def get_country_code(self):
    """
    Reads the fetched data as a pandas DataFrame.
    
    Returns:
    pandas.DataFrame: DataFrame containing the fetched data.
    """
    item = self.__get_DB()
    return item['Country_id']

  #Get dataset's reference period from hdx
  def get_ref_period(self):
    """
    Retrieves the reference period of a dataset from HDX.

    Returns:
    dict: The reference period of the dataset.
    """
    item = self.__get_DB()

    read_item = Dataset.read_from_hdx(item['Name'])  #get data from HDX
    read_item.set_expected_update_frequency('every week')  #updated
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
    mapfiles = self.__maps.list()['names']
    country_code = self.__get_DB()['Country_id']
    # Iterate through the list of files
    for country_file in mapfiles:
      # Check if country is found, extract the content of the file using get()
      if country_code.upper() in country_file:
        get_country_file = self.__maps.get(country_file).read()
        yield get_country_file

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
    # generate data from the filr
    for content in country_file:
      decoded = content.decode('utf-8')
      map_df = StringIO(decoded)
    # return a dataframe
    return gpd.read_file(map_df)
