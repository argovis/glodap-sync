import xarray as xr
from pymongo import MongoClient
import random, time, numpy

client = MongoClient('mongodb://database/argo')
db = client.argo

def tidylon(longitude):
    # map longitude on [0,360] to [-180,180], required for mongo indexing
    if longitude <= 180.0:
        return longitude;
    else:
        return longitude-360.0;

metadata = list(db['glodapMeta'].find({'_id': 'glodapv2.2016b'}))[0]

while True:
	latidx = random.randint(0,179)
	lonidx = random.randint(0,359)
	var = random.choice(['Cant', 'NO3', 'OmegaA', 'OmegaC', 'oxygen', 'pHts25p0', 'pHtsinsitutp', 'PI_TCO2', 'PO4', 'salinity', 'silicate', 'TAlk', 'TCO2', 'temperature'])
	ds = xr.open_dataset('/bulk/glodap/GLODAPv2.2016b.'+var+'.nc')
	element = random.choice([var, var+'_error', 'Input_mean', 'Input_std', 'Input_N', var+'_relerr'])
	renamed_element = element
	if not renamed_element.startswith(var):
		renamed_element = var + '_' + renamed_element
	print('checking', latidx, lonidx, renamed_element)

	# original data from netcdf
	column = ds[element][:,latidx,lonidx].to_dict()['data']

	# look up corresponding data from mongodb
	lon = tidylon(float(ds['lon'][lonidx].data) % 360) # mod since glodap is on [20,380]
	lat = float(ds['lat'][latidx].data)
	eltidx = metadata['data_info'][0].index(renamed_element)

	document = list(db['glodap'].find({'_id': str(lon)+'_'+str(lat)}))
	if(len(document) == 1):
		document = document[0]
		data = document['data'][eltidx]

		if not numpy.allclose(data, column, atol=0.00000001, equal_nan=True):
			print('mismatch', data, column)

		time.sleep(5)