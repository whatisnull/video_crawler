# -*- coding: utf-8 -*-
'''
Created on 2012-8-3

@author: wangwf
'''
import cookielib
from mypackage import urllib, urllib2, httplib2
import StringIO, gzip

global headers

socket_timeout = 20


headers = {
    'User-Agent'     : 'BFDSpider_INIT_A',
    'Accept'         : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-us,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Charset' : 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
    'Keep-Alive'     : '115',
    'Connection'     : 'keep-alive',
    'Cache-Control'  : 'max-age=0'
}

cookie = cookielib.CookieJar()
opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie))
urllib2.install_opener(opener)

def fetch_urllib2(url, data=None, ip=None, headers=headers):
    if not data and data == 'none':
        data =None
    
    request = urllib2.Request(url, data, headers)
    request.add_header("Accept-encoding", "gzip")
    usock = urllib2.urlopen(request, source_ip=ip, timeout=socket_timeout, host=None)
    response = usock.read()
    if usock.headers.get('content-encoding', None) == 'gzip':
        response = gzip.GzipFile(fileobj=StringIO.StringIO(response)).read()
        
    return usock.getcode(), usock.headers.get('content-type'), usock.headers.get('content-length'), response

def fetch_httplib2(url, method='GET', data=None, header=headers, cookies=None, referer=None, acceptencoding=None, proxy=None, ip=None, authority=None):
    if not data or data != 'none':
        data = None
    if cookies and cookies != 'none':
        header['Cookie'] = cookies
    if referer:
        header['referer'] = referer
    if acceptencoding == None or acceptencoding == 'default':
        header['Accept-Encoding'] = 'gzip, deflate'
    else:
        header['Accept-Encoding'] = acceptencoding
        
    conn = httplib2.Http(timeout=socket_timeout)
    conn.follow_redirects = True
    response, content = conn.request(uri=url, method=str(method).upper(), body=data,  headers=header,\
                                     redirections=5, connection_type=None, source_ip=ip, authority=authority)
    try:
        if response['-content-encoding'] == 'gzip':
            responses = gzip.GzipFile(fileobj=StringIO.StringIO(content)).read()
        else:
            responses = gzip.GzipFile(fileobj=StringIO.StringIO(content)).read()
    except:
        responses = content
    try:
        cookie = response['set-cookie']
    except:
        cookie = ''
    try:
        content_type = response['content-type']
    except:
        content_type = ''
    try:
        content_length = response['content-length']
    except:
        content_length = ''
    try:
        location = response['location']
    except:
        location = ''
        
    if headers.has_key('referer'):
        headers.pop('referer')
    if headers.has_key('Cookie'):
        headers.pop('Cookie')
    
    return response['status'], content_type, location, responses

