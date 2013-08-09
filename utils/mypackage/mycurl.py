# -*- coding: utf-8 -*-
import StringIO
import gzip
import pycurl
import re
import socket

class HostResolvedError(Exception):
    pass

class TimeoutError(Exception):
    pass

version_header_regexp = re.compile(r'^(Last-Modified|ETag):\s*(.*)$', re.I)
content_type_regexp = re.compile(r'^Content-Type:\s*([^;]*)($)|(;)', re.I)
charset_header_regexp = re.compile(r'^Content-Type:.*charset=(.+)($)|(;)|(\s)', re.I)
content_encoding_header_regexp = re.compile(r'^Content-Encoding:\s*([^;]*)($)|(;)', re.I)

class CurlResponse:
    timeout = 60 #seconds

    def __init__(self, curl_instance, url, request_headers=[], proxy=None):
        curl_instance.setopt(pycurl.URL, url)
        self.headers = {} # Response Headers

        curl_instance.setopt(pycurl.HTTPHEADER, request_headers)

        # 跟302/301 redirect
        curl_instance.setopt(pycurl.FOLLOWLOCATION, 1)
        # 最多跟5次跳转
        curl_instance.setopt(pycurl.MAXREDIRS, 5)
        curl_instance.setopt(pycurl.COOKIE, "") #auto process cookie
        curl_instance.fp = StringIO.StringIO()
        curl_instance.setopt(curl_instance.WRITEFUNCTION, curl_instance.fp.write)
        curl_instance.setopt(curl_instance.HEADERFUNCTION, self.write_header)
        curl_instance.setopt(pycurl.CONNECTTIMEOUT, self.timeout)
        curl_instance.setopt(pycurl.TIMEOUT, self.timeout)
        
        if proxy:
            ip, port = proxy.split(':', 1)
            curl_instance.setopt(pycurl.PROXY, ip)
            curl_instance.setopt(pycurl.PROXYPORT, int(port))
            curl_instance.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_SOCKS5)
        try:
            curl_instance.perform()
        except pycurl.error, e:
            # 47: Maximum (5) redirects followed
            # 56: Failure when receiving data from the peer
            if e[0] in (28, 47, 56):
                raise TimeoutError
            # 1: Protocol not supported or disabled in libcurl
            # 6: Can not resolve host
            # 7: No route to host
            elif e[0] in (1, 6, 7):
                raise HostResolvedError
            else:
                raise e

        self.body = curl_instance.fp.getvalue()
        if 'content-encoding' in self.headers and self.headers['content-encoding'].strip().lower() == 'gzip':
            self.body = gzip.GzipFile(fileobj=StringIO.StringIO(self.body)).read()
        self.status = curl_instance.getinfo(pycurl.HTTP_CODE)
        self.redirect_count = curl_instance.getinfo(pycurl.REDIRECT_COUNT)

        if self.redirect_count:
            self.effective_url = curl_instance.getinfo(pycurl.EFFECTIVE_URL)
        else:
            self.effective_url = None

    @property
    def current_version(self):
        if 'etag' in self.headers:
            return self.headers['etag']
        else:
            return self.headers.get('last-modified', None)

    def write_header(self, header):
        def match_header(name, pattern):
            match = pattern.match(header)
            if match:
                value = match.groups()[0]
                if value:
                    self.headers[name] = value.strip().lower()

        self.__match_version_header(header)
        match_header('content-type', content_type_regexp)
        match_header('charset', charset_header_regexp)
        match_header('content-encoding', content_encoding_header_regexp)
    
    def __match_version_header(self, header):
        match = version_header_regexp.match(header)
        if match:
            key, value = match.groups()
            self.headers[key.lower()] = value.strip()
    
    def __str__(self):
        return "Response[status=%s, redirect=%s]" % (self.status, self.redirect_count)


def get(url, timeout=60, request_headers=[], debug=False, proxy=None, bindip=None):
    if type(url) == unicode:
        url = str(url)
    def socketopen(family, socktype, protocol, bindip=bindip):
        s = socket.socket(family, socktype, protocol)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if bindip:
                s.bind((bindip, 0))
        except socket.error, msg:
            pass
        return s
    crl = pycurl.Curl()
    crl.setopt(pycurl.OPENSOCKETFUNCTION, socketopen)
    if debug:
        crl.setopt(pycurl.VERBOSE, 1)
    crl.setopt(pycurl.TIMEOUT, timeout)
    
    response = CurlResponse(crl, url, request_headers, proxy)
    response.request_url = url
    return response

def post(url, data, debug=False, proxy=None, bindip=None):
    if type(url) == unicode:
        url = str(url)
    def socketopen(family, socktype, protocol, bindip=bindip):
        s = socket.socket(family, socktype, protocol)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if bindip:
                s.bind((bindip, 0))
        except socket.error, msg:
            pass
        return s
    crl = pycurl.Curl()
    if debug:
        crl.setopt(pycurl.VERBOSE, 1)
    if type(data) == unicode:
        data = data.encode("utf8")
    crl.fp = StringIO.StringIO()
    crl.setopt(pycurl.POST, 1)
    crl.setopt(pycurl.POSTFIELDS, data)

    return CurlResponse(crl, url, proxy=proxy)

def _get(url, proxy=None, bindip=None):
    response = get(url, request_headers=[], proxy=proxy, bindip=bindip)
    return response.body

def _post(url, data, debug=False, proxy=None, bindip=None):
    response = post(url, data, debug=debug, proxy=proxy, bindip=bindip)
    return response.body

def _get_response(url, proxy=None, bindip=None):
    return get(url, request_headers=[], proxy=proxy, bindip=bindip)
