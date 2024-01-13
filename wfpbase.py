import logging
import pandas as pd
import requests
import os
from dotenv import load_dotenv
from io import StringIO
from deta import Deta
# from hdx.utilities.easy_logging import setup_logging
# from hdx.api.configuration import Configuration
# from hdx.facades.simple import facade
# from hdx.data.dataset import Dataset

#setup logging
#setup_logging()
#logger  = logging.getLogger(__name__)

#initialize HDX configuration
#Configuration.create(hdx_site="prod", user_agent="DSWFProject", hdx_read_only=True)

df = pd.read_csv(r"C:\Users\USER\Documents\DSWFProject\WFP_Files\wfp_countries_global_1.csv")

def read_data(data:str):
    """
    Retrieves information about food prices of a particular country from HDX.

    Parameters:
    data (str): A string value representing the data from the website 'https://https://data.humdata.org/dataset/.

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


def put_DB(base, data:str):   
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
        base.put(data=hdxdic)
        logger.info(f'Included {data} inside database')
    else:
        logger.error(f'Could not insert {data} into database')

def get_DB(base, key):
   """
   Utilises the GET method in order to get a data from the database using the key.
   
   Parameters:
   base: the database instance
   key: the item or record key

   Raises:
   A KeyError if key is invalid or not found.

   Returns:
   A collection of information about the record
   """

   fetch = base.fetch().items
   if key not in [item['key'] for i, item in enumerate(fetch)]:
       raise KeyError(f"Not found. Unknown key: {key}")
   return base.get(key)

def get_ref_period_DB(base, key):
   item = get_DB(base, key)
   read_item = read_data(item['Name'])
   return read_item.get_reference_period()

def get_data_from_link(base, key):
    item = get_DB(base, key)
    response = requests.get(url=item['Download_URL'], params={"downloadformat":"csv"})
    yield response.content.decode('utf-8')

def read_dataframe(base, key):
    gen = get_data_from_link(base, key)
    for con in gen:
        content = StringIO(con)
    df = pd.read_csv(content).drop(index=0)
    df = df.reset_index(drop=True)
    return df

# def global_file():
#    wfp = ddrive.get("wfp_countries_global_1.csv")
#    read = wfp.read()
#    content = read.decode('utf-8')
#    yield content

# def read_global_file():
#    for con in global_file():
#       content = StringIO(con)
#    return pd.read_csv(content)

