from .remote_files import *
from .read import parse_filename
import argparse
import datetime, calendar, time
from pathlib import Path
import os, sys
import logging
import multiprocessing
import xarray

BASEURL = 'ftp://apollo.eorc.jaxa.jp/pub/JASMES/Global_05km/uv[a-b]/daily'

def download_worker(tasks,id):
    logger = logging.getLogger('uvr.worker{}'.format(id))
    logger.debug('starting worker')
    while True:
        task = tasks.get()
        if task is None:
            logger.debug('stopping worker')
            break
        inname,outname = task
        logger.info('downloading {}'.format(inname))
        try:
            data = download(inname)
        except Exception as e:
            logger.error(e)
            continue
        logger.info('writing {}'.format(outname))
        data.to_netcdf(outname)

def main():
    now = datetime.datetime.now()
    parser = argparse.ArgumentParser(description='download UVR dataset')
    parser.add_argument('-n','--num-threads',type=int,default=4,help='number of download worker: default 4')
    parser.add_argument('-y','--year',type=int,default=now.year,help='download data for year, default={}'.format(now.year))
    parser.add_argument('-m','--month',type=int,default=now.month,help='download data for month, default={}'.format(now.month))
    parser.add_argument('-b','--base-url',default=BASEURL,help='base url, default {}'.format(BASEURL))
    parser.add_argument('-o','--output',metavar='DIR',help='write files to DIR')
    parser.add_argument('-d','--debug',action='store_true',default=False,help='log debug messages')
    parser.add_argument('-l','--log',metavar='LOG',help='log to file LOG rather than console')
    args = parser.parse_args()

    if args.month <1 or args.month>12:
        parser.error('month {} outside range 1-12'.format(args.month))
    if args.year > now.year:
        parser.error('year {} is in the future'.format(args.year))
    if args.year == now.year and args.month > now.month:
        parser.error('month {} of year {} is in the future'.format(args.month,args.year))

    # setup multiprocessing
    logger = logging.getLogger('uvr')
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.log is not None:
        lh = logging.FileHandler(args.log)
    else:
        lh = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    lh.setFormatter(formatter)
    logger.addHandler(lh)


    url = args.base_url
    if not url.endswith('/'):
        url += '/'
    url += '{:d}{:02d}/*.gz'.format(args.year,args.month)

    if args.output is not None:
        path = Path(args.output)
    else:
        path = Path('.')
    
    path = path.joinpath(str(args.year))

    #  check if dataset already exists
    ds = path.joinpath('{:d}{:02d}.nc'.format(args.year,args.month))
    logger.debug('checking if dataset {} already exists'.format(ds))
    if ds.exists():
        logger.info('dataset {:d}{:02d}.nc already exists'.format(args.year,args.month))
        sys.exit(0)

    if not path.exists():
        logger.debug('creating output directory {}'.format(path))
        os.makedirs(path)
    path = path.joinpath(str(args.month))
    if not path.exists():
        logger.debug('creating output directory {}'.format(path))
        os.makedirs(path)

    # start worker processes
    ctx = multiprocessing.get_context('fork')
    tasks = ctx.Queue()
    workers =[]
    for i in range(args.num_threads):
        p = ctx.Process(target=download_worker,args=(tasks,i))
        p.start()
        workers.append(p)

    for f in getFileNames(url):
        inname = Path(f).name
        if inname.endswith('.gz'):
            inname = inname[:-3]
        meta = parse_filename(inname)
        outname = '{date.year}{date.month:02d}{date.day:02d}_{var}.nc'.format(date=meta['start_date'],var=meta['variable'])
        outname = path.joinpath(outname)
        if outname.exists():
            logger.debug('data file {} already exists'.format(outname))
            continue

        tasks.put((f,outname))

    # sending poison pills
    for i in range(args.num_threads):
        tasks.put(None)
    # waiting for workers to finish
    logger.debug('waiting for workers')
    for p in workers:
        p.join()

    # check if we have all files for a month and merge them
    dataFiles = {}
    for f in path.iterdir():
        if f.name.endswith('.nc'):
            try:
                var = f.name[:-3].split('_')[1]
            except:
                logger.error('cannot extract variable name for file {}'.format(f.name))
                continue
            if var not in dataFiles:
                dataFiles[var] = 0
            dataFiles[var] += 1
    if len(dataFiles)>0:
        haveAllFiles = True
        numDays = calendar.monthrange(args.year,args.month)[1]
        for v in dataFiles:
            if dataFiles[v] != numDays:
                logger.info('not all datafiles for variable {} of {}-{} available'.format(v,args.year,args.month))
                haveAllFiles = False
    else:
        haveAllFiles = False

    if haveAllFiles:
        logger.info('merging datafiles for {}-{}'.format(args.year,args.month))
        ds = xarray.open_mfdataset(path.glob('*.nc'))
        outname = path.parent.joinpath('{}{:02d}.nc'.format(args.year,args.month))
        logger.info('writing {}'.format(outname))
        ds.to_netcdf(outname)
        ds.close()

        # removing individual datasets
        logger.info('removing partial datasets')
        for f in path.glob('*.nc'):
            f.unlink()
        path.rmdir()


if __name__ == '__main__':
    main()
