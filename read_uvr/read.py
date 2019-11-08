__all__ = ['read_uvr','uvr_from_buffer','parse_filename']

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

def parse_filename(name):
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

def uvr_from_buffer(fname,buf):
    """parse meta data from file name and get data from byte buffer"""
    data = None
    meta = parse_filename(fname)

    rlen = meta['pixel']*DTYPES[meta['dtype']][0]().nbytes
    npixel = int(buf[:6])
    nline  = int(buf[6:12])
    lon_min = float(buf[12:20]) 
    lat_max = float(buf[20:28])
    resolution = float(buf[28:36])
    slope = float(buf[36:48])
    offset = float(buf[48:60])
    para = buf[61:69].strip()
    outfile = buf[70:110].strip()
    
    assert npixel == meta['pixel']
    assert nline  == meta['line']


    raw_data = numpy.frombuffer(buf[rlen:],DTYPES[meta['dtype']][0]).reshape(1,nline,npixel)
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


def read_uvr(fname):
    """read dataset and produce an xarray dataset"""

    return uvr_from_buffer(fname.name,open(fname, mode='rb').read())

if __name__ == '__main__':
    import sys

    p = pathlib.Path(sys.argv[1])

    d = read_uvr(p)

    print (d)
    d.to_netcdf('test.nc')


    from matplotlib import pyplot
    import cartopy.crs as ccrs
    
    if True:
        pyplot.imshow(d.uvb[0,:,:])
    else:

        ax = pyplot.axes(projection=ccrs.PlateCarree(central_longitude=180.))
        #ax = pyplot.axes(projection=ccrs.OSGB())
        data = d.uvb.isel(time=0)
        data.plot.imshow(ax=ax, transform=ccrs.PlateCarree())
        ax.set_global()
        ax.coastlines()
    pyplot.show()
