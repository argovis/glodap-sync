# usage: python glodap-sync.py <first longitude index [0,360)> <last longitude index [0,360)> 
import xarray as xr
from pymongo import MongoClient
import datetime, math, sys, itertools, numpy
from geopy import distance

def tidylon(longitude):
    # map longitude on [0,360] to [-180,180], required for mongo indexing
    if longitude <= 180.0:
        return longitude;
    else:
        return longitude-360.0;

def find_basin(basins, lon, lat):
    # for a given lon, lat,
    # identify the basin from the lookup table.
    # choose the nearest non-nan grid point.

    gridspacing = 0.5

    basin = basins['BASIN_TAG'].sel(LONGITUDE=lon, LATITUDE=lat, method="nearest").to_dict()['data']

    if math.isnan(basin):
        # nearest point was on land - find the nearest non nan instead.
        lonplus = math.ceil(lon / gridspacing)*gridspacing
        lonminus = math.floor(lon / gridspacing)*gridspacing
        latplus = math.ceil(lat / gridspacing)*gridspacing
        latminus = math.floor(lat / gridspacing)*gridspacing
        grids = [(basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonplus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonplus)).miles)]

        grids = [x for x in grids if not math.isnan(x[0])]
        if len(grids) == 0:
            # all points on land
            #print('warning: all surrounding basin grid points are NaN')
            basin = -1
        else:
            grids.sort(key=lambda tup: tup[1])
            basin = grids[0][0]

    return int(basin)

client = MongoClient('mongodb://database/argo')
db = client.argo

basins = xr.open_dataset('parameters/basinmask_01.nc')
glodapvars = ['Cant', 'NO3', 'OmegaA', 'OmegaC', 'oxygen', 'pHts25p0', 'pHtsinsitutp', 'PI_TCO2', 'PO4', 'salinity', 'silicate', 'TAlk', 'TCO2', 'temperature']

if sys.argv[1] == 'meta':
	# metadata & summary construction

	metadata = {
		'_id': 'glodapv2.2016b',
		'data_type': 'glodap',
		'date_updated_argovis': datetime.datetime.now(),
		'source': [{
			'source': ['GLODAPv2.2016b'],
			'url': 'https://glodap.info/index.php/mapped-data-product/',
			'doi': '10.5194/essd-8-325-2016'
		}],
		'levels': [0.0, 10.0, 20.0, 30.0, 50.0, 75.0, 100.0, 125.0, 150.0, 200.0, 250.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1100.0, 1200.0, 1300.0, 1400.0, 1500.0, 1750.0, 2000.0, 2500.0, 3000.0, 3500.0, 4000.0, 4500.0, 5000.0, 5500.0],
                'level_units': "m",
		'data_info': [
			[],
			['units', 'long_name'],
			[]
		],
		'snr': {},
		'cl': {},
		'lattice': {
			"center" : [
				0.5,
				0.5
			],
			"spacing" : [
				1,
				1
			],
			"minLat" : -77.5,	# double check on update
			"minLon" : -179.5,
			"maxLat" : 89.5,
			"maxLon" : 179.5
		}
	}

	summary = {
		'_id': 'glodapsummary',
		'data': [],
		'lattice': []
	}
	for var in glodapvars:
		ds = xr.open_dataset('/bulk/glodap/GLODAPv2.2016b.'+var+'.nc')
		metadata['snr'][var] = ds['SnR'][0].to_dict()['data']
		metadata['cl'][var] = ds['CL'][0].to_dict()['data']
		# each file has a measurement, error, input mean, input std, input n and relative error
		elements = [var, var+'_error', 'Input_mean', 'Input_std', 'Input_N', var+'_relerr']
		renamed_elements = [var, var+'_error', var+'_Input_mean', var+'_Input_std', var+'_Input_N', var+'_relerr']
		metadata['data_info'][0] += renamed_elements
		for e in elements:
			metadata['data_info'][2].append( [ ds[e].attrs['units'], ds[e].attrs['long_name'] ] )
	summary['data'] = metadata['data_info'][0]
	summary['lattice'] = [x['geolocation']['coordinates'] for x in list(db['glodap'].find({},{'geolocation':1}))]
		
	try:
		db['glodapMeta'].replace_one({'_id': 'glodapv2.2016b'},metadata, True)
		db['summaries'].replace_one({'_id': 'glodapsummary'},summary, True)
	except BaseException as err:
		print('error: db write failure')
		print(err)
else:
	# data construction
	data = []
	ds = xr.open_dataset('/bulk/glodap/GLODAPv2.2016b.Cant.nc')
	for lonidx in range(360):
		data.append([])
		for latidx in range(180):
			lon = tidylon(float(ds['lon'][lonidx].data) % 360) # mod since glodap is on [20,380]
			lat = float(ds['lat'][latidx].data)
			data[lonidx].append({
				'_id': str(lon)+'_'+str(lat),
				'metadata': ['glodapv2.2016b'],
				'geolocation': {"type":"Point", "coordinates":[lon,lat]},
				'basin': find_basin(basins, lon, lat),
				'timestamp': datetime.datetime(year=1000, month=1, day=1),
				'data': []
			})

	# made the whole map above, but only populate some of it to sneak under memory constraints
	start_lonidx = int(sys.argv[1])
	end_lonidx = int(sys.argv[2])

	for var in glodapvars:
		print('var extraction', var)
		ds = xr.open_dataset('/bulk/glodap/GLODAPv2.2016b.'+var+'.nc')
		elements = [var, var+'_error', 'Input_mean', 'Input_std', 'Input_N', var+'_relerr']
		for lonidx in range(start_lonidx, end_lonidx):
			print('longitude idx', lonidx)
			for latidx in range(180):
				for e in elements:
					data[lonidx][latidx]['data'].append(ds[e][:,latidx,lonidx].to_dict()['data'])

	for lonidx in range(start_lonidx, end_lonidx):
		for latidx in range(180):
			if not numpy.isnan(list(itertools.chain.from_iterable(data[lonidx][latidx]['data']))).all():
				try:
					db['glodap'].insert_one(data[lonidx][latidx])
				except BaseException as err:
					print('error: db write failure')
					print(err)
					print(data[lonidx][latidx])



