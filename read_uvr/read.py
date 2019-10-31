import numpy
import xarray
import pathlib
import datetime
import numpy.ma

INSTRUMENTS = {
    'MOD' : 'Terra MODIS',
    'MYD' : 'Aqua MODIS',
    'MDS' : 'Terra & Aqua MODIS average',
    'SWF' : 'SeaWiFS',
}

AVERAGE = {
    'Av1' : 'daily average',
    'Avm' : 'monthly average',
    'Avh' : 'half-month average'
}

VARIABLES = {
    'par'  : ('daily PAR', 'Ein/m^2/day'),
    'dpar' : ('direct PAR',''),
    'swr'  : ('mean shortwave radiation', 'W/m^2'),
    'tip'  : ('transmittance of instantaneous PAR at noon',''),
    'uva'  : ('UVA','W/m^2'),
    'uvb'  : ('UVB','W/m^2'),
    'rpar' : ('surface reflectance weighted by PAR wavelengths & solar irradiance',''),
    'lst'  : ('surface temperature (not validated)','')
}

DTYPES = {
    'le' : (numpy.int16,-1),
    '8b' : (numpy.uint8,255)
}

def parse_filenamef(name):
    """parse the file name of the dataset"""
    NCHAR = 44
    if len(name)!=NCHAR:
        raise ValueError('Expected string of length {}'.format(NCHAR))
    
    meta = {}
    inst = name[:3]
    if inst not in INSTRUMENTS:
        raise ValueError("parsing {}: unknown instrument {}".format(name,inst))
    meta['instrument'] = inst
    meta['start_date'] = datetime.datetime.strptime(name[10:18],'%Y%m%d')
    avg =name[18:21]
    if avg not in AVERAGE:
        raise ValueError("parsing {}: unknown average {}".format(name,avg))
    meta['average'] = avg
    pixel = name[27:31]
    try:
        meta['pixel'] = int(pixel)
    except:
        raise ValueError("parsing {}: cannot parse number of pixels {}".format(name,pixel))
    line = name[32:36]
    try:
        meta['line'] = int(line)
    except:
        raise ValueError("parsing {}: cannot parse number of lines {}".format(name,line))
    var = name[37:41].replace('_','')
    if var not in VARIABLES:
        raise ValueError("parsing {}: unknown variable {}".format(name,var))
    meta['variable'] = var
    dt = name[42:44]
    if dt not in DTYPES:
        raise ValueError("parsing {}: unknown data type {}".format(name,dt))
    meta['dtype'] = dt

    return meta

def read_uvr(fname):
    """read dataset and produce an xarray dataset"""
    data = None
    meta = parse_filenamef(fname.name)

    with open(fname, mode='rb') as infile:
        # read header
        rlen = meta['pixel']*DTYPES[meta['dtype']][0]().nbytes
        d = infile.read(rlen)
        npixel = int(d[:6])
        nline  = int(d[6:12])
        lon_min = float(d[12:20]) 
        lat_max = float(d[20:28])
        resolution = float(d[28:36])
        slope = float(d[36:48])
        offset = float(d[48:60])
        para = d[61:69].strip()
        outfile = d[70:110].strip()

        assert npixel == meta['pixel']
        assert nline  == meta['line']

        raw_data = numpy.frombuffer(infile.read(),DTYPES[meta['dtype']][0]).reshape(1,nline,npixel)
        raw_data = numpy.flip(raw_data,axis=1)
        mask = numpy.where(raw_data == DTYPES[meta['dtype']][1], True, False)
        raw_data = numpy.ma.array(offset+slope*raw_data,mask=mask)
        
        time = xarray.Variable(('time',), [meta['start_date']])
        lat = xarray.Variable(('lat',),numpy.arange(nline)*resolution-lat_max)
        lon = xarray.Variable(('lon',),lon_min+numpy.arange(npixel)*resolution)

        d = xarray.Variable(('time','lat','lon'),
                            raw_data,
                            attrs = {'description': VARIABLES[meta['variable']][0],
                                     'units': VARIABLES[meta['variable']][1]
                                 },
                            encoding = {'_FillValue':   DTYPES[meta['dtype']][1],
                                        'scale_factor': slope,
                                        'add_offset': offset,
                                        'dtype': DTYPES[meta['dtype']][0]}
                        )
        data = xarray.Dataset({'time':time,
                               'lat' : lat,
                               'lon' : lon,
                               meta['variable'] : d})

    return data

if __name__ == '__main__':
    import sys

    p = pathlib.Path(sys.argv[1])

    d = read_uvr(p)

    print (d)
    d.to_netcdf('test.nc')


    from matplotlib import pyplot
    import cartopy.crs as ccrs
    
    if False:
        pyplot.imshow(d.uvb[0,:,:])
    else:

        ax = pyplot.axes(projection=ccrs.PlateCarree(central_longitude=180.))
        #ax = pyplot.axes(projection=ccrs.OSGB())
        data = d.uvb.isel(time=0)
        data.plot.imshow(ax=ax, transform=ccrs.PlateCarree())
        ax.set_global()
        ax.coastlines()
    pyplot.show()
