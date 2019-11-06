import urllib
import pathlib
from ftplib import FTP
from .read import uvr_from_buffer
import gzip

def getFileNames(url):
    """get a list of files names from url which can include wildcards"""

    # find first occurance of a wild card
    u = url.split('/')
    have_wildcard = None
    wildcard = ''
    url_remainder = ''
    for i in range(len(u)):
        if any([w in u[i] for w in ['[','*','?']]):
            have_wildcard = i
            break
    if have_wildcard is not None:
        url = '/'.join(u[:have_wildcard])
        wildcard = u[have_wildcard]
        url_remainder = '/'.join(u[have_wildcard+1:])
        
    scheme,netloc,path,_,_,_ = urllib.parse.urlparse(url)

    assert scheme == 'ftp'
    
    ftp = FTP(netloc)
    ftp.login()
    try:
        ftp.cwd(path)
    except Exception as e:
        print (e)
        return

    for e in ftp.mlsd():
        if pathlib.PurePath(e[0]).match(wildcard):
            if len(url_remainder)>0:
                for f in getFileNames('/'.join([url,e[0],url_remainder])):
                    yield f
            else:
                yield '/'.join([url,e[0]])

def download(url):
    """download data file"""
    
    data = None
    fname=pathlib.Path(url).name
    is_zipped = fname.endswith('.gz')
    if is_zipped:
        fname = fname[:-3]
    with urllib.request.urlopen(u) as f:
        if is_zipped:
            raw_data = gzip.GzipFile(fileobj=f).read()
        else:
            raw_data = f.read()
        data = uvr_from_buffer(fname,raw_data)

    return data

if __name__ == '__main__':

    if False:
        u = 'ftp://apollo.eorc.jaxa.jp/pub/JASMES/Global_05km/uv[a-b]/daily/201911/*.gz'
        for f in getFileNames(u):
            print (f)
    else:
        u = 'ftp://apollo.eorc.jaxa.jp/pub/JASMES/Global_05km/uvb/daily/201911/MOD02SSH_A20191105Av1_v811_7200_3601_uvb__le.gz'
        d = download(u)
        print (d)
