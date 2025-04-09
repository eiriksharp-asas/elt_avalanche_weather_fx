# Importation of Python modules
from datetime import time 
from datetime import datetime, timedelta
import re
import warnings
import numpy as np
from owslib.wms import WebMapService
import pandas as pd
from openpyxl import Workbook
import logging

# Ignore warnings from the OWSLib module
warnings.filterwarnings('ignore', module='owslib', category=UserWarning)

logging.basicConfig(level=logging.INFO, filename='run_log.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logger=logging.getLogger() 
logger.info('Start time: ' +  str(datetime.today()))

#output_path = "C:\\Users\\Eirik Sharp\\Avalanche Services\\Operations - Remote Programs\\ZArchive\\LSCLP_Wx\\01ImmediateForecast"
output_path = "D:\\ETL_output\\wx"

# Set up forecast parameters
# Layers:
'''layers = [
    'GDPS.ETA_TT', #Air temperature [°C]
    'GDPS.ETA_PN-SLP', #Sea level pressure [Pa]
    'GDPS.ETA_HR', # Relative humidity [%]
    'GDPS.ETA_RN', # Rain accumulation [kg/(m^2)]
    'GDPS.ETA_SN', # Snow accumulation [kg/(m^2)
    'GDPS.PRES_WD.800.3h', # Wind direction at 850.0 mb (3 hourly forecast) [°]
    'GDPS.PRES_WSPD.800.3h', # Winds at 800.0 mb (3 hourly forecast) [m/s]   
    'GEPS.DIAG.3_TT.ERMEAN', #Air temperature at 2 m above ground [°C] (mean)
    'GEPS.DIAG.3_TT.ERC25', # Air temperature at 2 m above ground [°C] (25th percentile)
    'GEPS.DIAG.3_TT.ERC75', # Air temperature at 2 m above ground [°C] (75th percentile)
    'GEPS.DIAG.3_WCF.ERMEAN', # Wind chill factor at 2 m above ground [°C] (mean)
    'GEPS.DIAG.24_PRMM.ERGE1', # Quantity of precipitation >= 1 mm [probability %]
    'GEPS.DIAG.24_RNMM.ERGE1', # Rain >= 1 mm [probability %]
    'GEPS.DIAG.24_RNMM.ERGE10', # Rain >= 10 mm [probability %]
    'GEPS.DIAG.24_RNMM.ERGE25', # Rain >= 25 mm [probability %]    
    'GEPS.DIAG.24_RNMM.ERMEAN', # Rain (mean)
    'GEPS.DIAG.24_RNMM.ERC25', # Rain (25th percentile)
    'GEPS.DIAG.24_RNMM.ERC75', # Rain (75th percentile)
    'GEPS.DIAG.24_SNMM.ERGE1', # Snow >= 1 mm [probability %]
    'GEPS.DIAG.24_SNMM.ERGE10', # Snow >= 10 mm [probability %]
    'GEPS.DIAG.24_SNMM.ERGE25', # Snow >= 25 mm [probability %]   
    'GEPS.DIAG.24_SNMM.ERMEAN', # Snow (mean)
    'GEPS.DIAG.24_SNMM.ERC25', # Snow (25th percentile)
    'GEPS.DIAG.24_SNMM.ERC75' # Snow (75th percentile)
]'''

layers = ['GDPS.ETA_TT']

# Local time zone (in winter PST is UTC-08:00):
time_zone = -8

# set model run time
current_date = datetime.today().strftime('%Y-%m-%d')
geps_run = current_date+'T00:00:00Z'

#define forecast range
fx_range = pd.date_range(geps_run,periods=16)
#fx_range = [date_time.strftime('%Y-%m-%d') for date_time in fx_range]
fx_range = [date_time.date() for date_time in fx_range]

# Station details:
# North Hirsh: y = 54.072, x = -128.273
# Icy Pass: y = 54.038, x = -128.055
north_hirsh = {'name': "NorthHirsch", 'x': -128.273, 'y': 54.072}
#icy_pass = {'name': "IcyPass", 'x': -128.055, 'y': 54.038}
stations = [north_hirsh]


# WMS service connection
wms = WebMapService('https://geo.weather.gc.ca/geomet?SERVICE=WMS' + '&REQUEST=GetCapabilities', version='1.3.0', timeout=300)

def correct_wind(station,d):
    dirs = ["~"]
    cwind = 0
    if station == 'NorthHirsch':
        dirs = dirs = ["E", "W"]
        cwind = int((d+45)/180)
    if station == 'IcyPass':
        dirs = ["NE", "SW"]
        cwind = int((d + 45)/180)
    return dirs[cwind % 2]

def ms_to_windspeed(m):
    if (m<=1):
        return 'C'
    if (m>1 and m<=7):
        return 'L'
    if (m>7 and m<=11):
        return 'M'
    if (m>11 and m<=17):
        return 'S'
    if (m>17):
        return 'X'

def time_parameters(layer):
    start_time, end_time, interval = (wms[layer].dimensions['time']['values'][0].split('/'))
    iso_format = '%Y-%m-%dT%H:%M:%SZ'
    start_time = datetime.strptime(start_time, iso_format)
    end_time = datetime.strptime(end_time, iso_format)
    interval = int(re.sub(r'\D', '', interval))
    return start_time, end_time, interval

def request(layer):
    info = []
    pixel_value = []
    try:
        for timestep in time:
            # WMS GetFeatureInfo query
            info.append(wms.getfeatureinfo(layers=[layer],
                                        srs='EPSG:4326',
                                        bbox=(min_x, min_y, max_x, max_y),
                                        size=(100, 100),
                                        format='image/jpeg',
                                        query_layers=[layer],
                                        info_format='text/plain',
                                        xy=(50, 50),
                                        feature_count=1,
                                        time=str(timestep.isoformat()) + 'Z'
                                        ))
            # Probability extraction from the request's results
            text = info[-1].read().decode('utf-8')
            pixel_value.append(str(re.findall(r'value_0\s+\d*.*\d+', text)))
            try:
                pixel_value[-1] = float(re.sub('value_0 = \'', '', pixel_value[-1]).strip('[""]'))
            except:
                pixel_value[-1] = 0.0
    except:
        logger.exception('')
        pixel_value = "~"
    return pixel_value

for station in stations:
    logger.info('Station: ' +  station['name'] + "...")
    fx_table=pd.DataFrame(index=fx_range)

    x=station['x']
    y=station['y']

    # bbox parameter
    min_x, min_y, max_x, max_y = x - 0.25, y - 0.25, x + 0.25, y + 0.25

    # dictonary to save GEPS forecast data
    fx_data = {}
    # dataframe for agregated forecast data
    fx_table=pd.DataFrame(index=fx_range)
        
    # agregated forecast data
    for layer in layers:
        logger.info('Layer: ' +  layer + "...")
        logger.info('Commence wmw call: ' +  str(datetime.today()))
        start_time, end_time, interval = time_parameters(layer)
        time = [start_time]
        while time[-1] < end_time:
            time.append(time[-1] + timedelta(hours=interval))
        fx = pd.DataFrame()
        fx['time']=time
        fx['value']=request(layer)
        logger.info('Complete wmw call: ' +  str(datetime.today()))
        if (layer == 'GDPS.ETA_TT'
            or layer=='GEPS.DIAG.3_TT.ERMEAN'
            or layer == 'GEPS.DIAG.3_TT.ERC25'
            or layer == 'GEPS.DIAG.3_TT.ERC75'
            or layer=='GEPS.DIAG.3_WCF.ERMEAN'):      
            fx['date']=[date_time.date() for date_time in fx['time']]
            fx_table[layer+'_min']=fx.groupby([fx.date])['value'].min().round(0)
            fx_table[layer+'_max']=fx.groupby([fx.date])['value'].max().round(0)
        if (layer == 'GDPS.ETA_PN-SLP'
            or layer=='GDPS.ETA_HR'):
            fx['date']=[date_time.date() for date_time in fx['time']]
            fx_table[layer+'_mean']=fx.groupby([fx.date])['value'].mean().round(0)
        if (layer=='GDPS.ETA_RN'
            or layer == 'GDPS.ETA_SN'):
            fx['date']=[date_time.date() for date_time in fx['time']]
            fx_table[layer+'_total']=fx.groupby([fx.date])['value'].sum().round(0)
        if layer=='GDPS.PRES_WD.800.3h':
            fx['date']=[date_time.date() for date_time in fx['time']]
            fx['value']=fx['value'].apply(lambda x: correct_wind(station['name'], x))
            fx_table[layer]=fx.groupby([fx.date])['value'].agg(pd.Series.mode).to_frame()
        if layer=='GDPS.PRES_WSPD.800.3h':
            fx['date']=[date_time.date() for date_time in fx['time']]
            fx['value']=fx['value'].apply(lambda x: ms_to_windspeed(x))
            fx_table[layer]=fx.groupby([fx.date])['value'].agg(pd.Series.mode).to_frame()
        if (layer=='GEPS.DIAG.24_PRMM.ERGE1'
            or layer=='GEPS.DIAG.24_RNMM.ERGE1'
            or layer=='GEPS.DIAG.24_RNMM.ERGE10'
            or layer=='GEPS.DIAG.24_RNMM.ERGE25'
            or layer=='GEPS.DIAG.24_RNMM.ERC25'
            or layer=='GEPS.DIAG.24_RNMM.ERC75'
            or layer=='GEPS.DIAG.24_RNMM.ERMEAN'
            or layer=='GEPS.DIAG.24_SNMM.ERGE1'
            or layer=='GEPS.DIAG.24_SNMM.ERGE10'
            or layer=='GEPS.DIAG.24_SNMM.ERGE25'
            or layer=='GEPS.DIAG.24_SNMM.ERC25'
            or layer=='GEPS.DIAG.24_SNMM.ERC75'
            or layer=='GEPS.DIAG.24_SNMM.ERMEAN'):
            fx['time']=[date_time+timedelta(days=-1) for date_time in fx['time']]
            fx=fx.set_index('time')
            fx_table[layer]=fx.at_time('00:00').round(0)

            
    # saving the DataFrame as a CSV file

    try:
        csv_data = fx_table.T.to_csv(output_path + station['name']+'.csv', index = True, mode='w+')
        logger.info(csv_data)
    except:
        logger.exception('')


    # with pd.ExcelWriter("LSCLP_CGL8W_ImmediateWeatherForecast_Data.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    #     fx_table.to_excel(writer, station['name'], index=True)
    
    logging.info('End Time: ' +  str(datetime.today()))