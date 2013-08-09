# -*- coding: utf-8 -*-

import sys, os
reload(sys)
sys.setdefaultencoding("utf-8")
f_path = os.path.dirname(__file__)
if len(f_path) < 1: f_path = "."
sys.path.append(f_path)
sys.path.append(f_path + "/..")

from utils.dataSaver import DataSaver
from Queue import PriorityQueue
from time import sleep
from utils.mylogger import logging
import time
import threading
import traceback
import json
import random
from utils.BeautifulSoup import BeautifulSoup as bs
import urllib
from utils.fetchTools import fetch_httplib2 as fetch
import base64, zlib
import re
from random import randint
from urllib2 import HTTPError
from urlparse import urlparse
from xml.dom.minidom import parseString


log = logging.getLogger("crawler")

LIST_URL_TYPE = 'LIST_URL'
ITEM_URL_TYPE = 'ITEM_URL'
REAL_URL_TYPE = 'REAL_URL'

PARSE_TYPE = 1

MAX_TRY = 10

BASE_URL = 'http://v.baidu.com/movie_intro/?dtype=moviePlaySource&service=json&id=%d'
SUPPORT_URL = 'http://v.baidu.com/v?rn=10&word=%s&ct=905969666'


BAIY_HOST = 'www.baiy.net'
AIPAI_HOST = 'www.aipai.com'
WL_HOST = 'www.56.com'
UK_HOST = 'v.youku.com'
SOHU_HOST = 'tv.sohu.com'
KUSIX_HOST = 'v.ku6.com'
TUDOU_HOST = 'www.tudou.com'


def zip_data(data):
    return base64.b64encode(zlib.compress(data, zlib.Z_BEST_SPEED))

def GetListData(listurl, times=0):
    res = []
    if times > MAX_TRY:
        return res
    try:
        _, _, _, response = fetch(listurl)
        jdata = json.loads(response)
        videos = jdata['videoshow']['videos']
        for video in videos:
            rating = video['rating']
            title = video['title']
            url = video['url']
            source = video['source']
            area = ' '.join([d['name'] for d in video['area']])
            actor = ' '.join([d['name'] for d in video['actor']])
            cid = video['id']
            duration = video['duration']
            intro = video['intro']
            s_intro = video['s_intro']
            date = video['date']
            ctype = ' '.join([t['name']for t in video['type']])
            imgh_url = video['imgh_url']
            imgv_url = video['imgv_url']
            res.append([rating, title, url, source, area, actor, cid, duration, intro, s_intro, date, ctype, imgh_url, imgv_url])
        return res
    except:
        t, v, tb = sys.exc_info()
        log.error("GetListData listurl:  %s %s,%s,%s" % (listurl, t, v, traceback.format_tb(tb)))
        return GetListData(listurl, times + 1)

def GetItemData(itemurl, parsetype, times=0):
    res = []
    if times > MAX_TRY:
        return res
    try:
        _, _, _, response = fetch(itemurl)
        if parsetype == PARSE_TYPE:
            jdata = json.loads(response)
            videos = jdata['video']
            for video in videos:
                playlink = video['playlink']
                njdata = json.loads(playlink)
                playurl = njdata['links']['0']['url']
                anchor = njdata['links']['0']['anchor']
                res.append([playurl, anchor, ''])
        else:
            response = response.decode('gb18030')
            response = re.sub('tag: \[.*.\]', '', response)
            playtimes = re.findall('(?<=duration_hour\: \")\d+\:\d+(?=\"\,)', response)
            playurls = re.findall('(?<=url\: \").*.(?=\"\,)', response)
            ti = re.findall('(?<=ti\:\").*.(?=\"\,)', response)
            datas = zip(playurls, ti, playtimes)
            res.extend(datas)
        return res
    
    except:
        t, v, tb = sys.exc_info()
        log.error("GetItemData itemurl:  %s, %s,%s,%s" % (itemurl, t, v, traceback.format_tb(tb)))
        return GetItemData(itemurl, parsetype, times + 1)

def GetBaiyRealUrl(playurl, times=0):
    res = []
    if times > MAX_TRY:
        return res
    try:
        _, _, _, response = fetch(playurl)
        soup = bs(response)
        playlist = soup.findAll('ul', id="playlist")
        if playlist:
            newplayurl = playlist[0].script['src']
            if newplayurl:
                url = 'http://' + BAIY_HOST + newplayurl
                _, _, _, response = fetch(url)
                uri = re.findall("(?<=unescape\(').*.(?='\);)", response)[0]
                info = urllib.unquote(uri)
                for s in info.split('$$$'):
                    res.extend([s.split('$')])
        return res
    except:
        t, v, tb = sys.exc_info()
        log.error("GetBaiyRealUrl playurl:  %s, %s,%s,%s" % (playurl, t, v, traceback.format_tb(tb)))
        return GetBaiyRealUrl(playurl, times + 1)
    
def GetAiPaiRealUrl(playurl, times=0):
    res = []
    if times > MAX_TRY:
        return res
    try:
        _, _, _, response = fetch(playurl)
        assetpurl = re.findall("(?<=asset_pUrl \= \').*.(?=\'\;)", response)
        if assetpurl:
            realurl = assetpurl[0].replace('iphone.aipai.com/', '').replace('card.m3u8', 'card.flv')
            res.append(['', realurl])
        return res
    except:
        t, v, tb = sys.exc_info()
        log.error("GetAiPaiRealUrl playurl:  %s, %s,%s,%s" % (playurl, t, v, traceback.format_tb(tb)))
        return GetAiPaiRealUrl(playurl, times + 1)
    
def GetWlRealUrl(playurl, times=0):
    res = []
    if times > MAX_TRY:
        return res
    try:
        _, _, _, response = fetch(playurl)
        oflvo = re.findall('(?<=var _oFlv_o \= )\{.*.\}(?=;)', response)
        if not oflvo:
            return res
        
        jdata = json.loads(oflvo[0])
        pid = jdata['id']
#         pid = re.findall('(?<=var _oFlv_o \= \{\"id\"\:\")\d+(?=\",\")', response)
        if pid:
#             pid = pid[0]
            url = 'http://vxml.56.com/json/%d/?src=site' % (int(pid))
            _, _, _, response = fetch(url)
            jdata = json.loads(response)
            rfiles = jdata['info']['rfiles']
            for rf in rfiles:
                realurl = rf['url']
                playtype = rf['type']  # 可能是清晰度
                res.append(['', realurl])
        return res
    except:
        t, v, tb = sys.exc_info()
        log.error("GetWlRealUrl playurl:  %s, %s,%s,%s" % (playurl, t, v, traceback.format_tb(tb)))
        return GetWlRealUrl(playurl, times + 1)

def FindUKVideo(info, stream_type=None, times=0):
    segs = info['data'][0]['segs']
    types = segs.keys()
    if not stream_type:
        for x in ['hd2', 'mp4', 'flv']:
            if x in types:
                stream_type = x
                break
        else:
            raise NotImplementedError()
    file_type = {'hd2':'flv', 'mp4':'mp4', 'flv':'flv'}[stream_type]

    seed = info['data'][0]['seed']
    source = list("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ/\\:._-1234567890")
    mixed = ''
    while source:
        seed = (seed * 211 + 30031) & 0xFFFF
        index = seed * len(source) >> 16
        c = source.pop(index)
        mixed += c

    ids = info['data'][0]['streamfileids'][stream_type].split('*')[:-1]
    vid = ''.join(mixed[int(i)] for i in ids)

    sid = '%s%s%s' % (int(time.time() * 1000), randint(1000, 1999), randint(1000, 9999))

    urls = []
    for s in segs[stream_type]:
        no = '%02x' % int(s['no'])
        url = 'http://f.youku.com/player/getFlvPath/sid/%s_%s/st/%s/fileid/%s%s%s?K=%s&ts=%s' % (sid, no, file_type, vid[:8], no.upper(), vid[10:], s['k'], s['seconds'])
        urls.append((url, int(s['size'])))
    return urls

def GetUKInfo(videoId2, times=0):
    if times > MAX_TRY:
        return None
    try:
        url = 'http://v.youku.com/player/getPlayList/VideoIDS/%s' % (videoId2)
        _, _, _, response = fetch(url)
        return json.loads(response)
    except:
        return GetUKInfo(videoId2, times + 1)

def GetUKouRealUrl(playurl, times=0):
    res = []
    if times > MAX_TRY:
        return res
    try:
        _, _, _, response = fetch(playurl)
        id2 = re.search(r"var\s+videoId2\s*=\s*'(\S+)'", response).group(1)
        info = GetUKInfo(id2)
        urls, _ = zip(*FindUKVideo(info, stream_type=None))
        if len(urls) == 1:
            url = urls[0]
            _, _, location, response = fetch(url)
            res.append(['', location])
        else:
            for url in urls:
                _, _, location, response = fetch(url)
                res.append(['', location])
                time.sleep(2)
        return res
    except:
        t, v, tb = sys.exc_info()
        log.error("GetUKouRealUrl playurl:  %s, %s,%s,%s" % (playurl, t, v, traceback.format_tb(tb)))
        return GetUKouRealUrl(playurl, times + 1)

def GetSoHuInfo(host, prot, tfile, new, times=0):
    if times > MAX_TRY:
        return 
    try:
        url = 'http://%s/?prot=%s&file=%s&new=%s' % (host, prot, tfile, new)
        _, _, _, response = fetch(url)
        start, _, host, key, _, _, _, _ = response.split('|')
        return '%s%s?key=%s' % (start[:-1], new, key)
    except:
        t, v, tb = sys.exc_info()
        log.error("GetSoHuInfo %s,%s,%s" % (t, v, traceback.format_tb(tb)))
        return GetSoHuInfo(host, prot, tfile, new, times + 1)

def GetSoHuRealUrl(playurl, times=0):
    res = []
    if times > MAX_TRY:
        return res
    try:
        _, _, _, response = fetch(playurl)
        vid = re.search('vid="(\d+)', response).group(1)
        newurl = 'http://hot.vrs.sohu.com/vrs_flash.action?vid=%s' % vid
        _, _, _, response = fetch(newurl)
        jdata = json.loads(response)
        host = jdata['allot']
        prot = jdata['prot']
        urls = []
        data = jdata['data']
        title = data['tvName']
        size = sum(data['clipsBytes'])
        for tfile, new in zip(data['clipsURL'], data['su']):
            urls.append(GetSoHuInfo(host, prot, tfile, new))
        if len(urls) == 1:
            url = urls[0]
            res.append(['', url])
        else:
            for url in urls:
                res.append(['', url])
        return res
    except:
        t, v, tb = sys.exc_info()
        log.error("GetSoHuRealUrl playurl:  %s, %s,%s,%s" % (playurl, t, v, traceback.format_tb(tb)))
        return GetSoHuRealUrl(playurl, times + 1)

def GetKuSixRealUrl(playurl, times=0):
    res = []
    if times > MAX_TRY:
        return res
    try:
        _, _, _, response = fetch(playurl)
        data = re.findall('data: {.*.} }\,', response)
        if data:
            data = data[0][5:-2]
            jdata = json.loads(data)
            t = jdata['data']['t']
            f = jdata['data']['f']
            size = jdata['data']['videosize']
            res.append(['', f])
        return res
    except:
        t, v, tb = sys.exc_info()
        log.error("GetKuSixRealUrl playurl:  %s, %s,%s,%s" % (playurl, t, v, traceback.format_tb(tb)))
        return GetKuSixRealUrl(playurl, times + 1)

def GetTuDouRealUrl(playurl, times=0):
    res = []
    if times > MAX_TRY:
        return res
    try:
        _, _, _, response = fetch(playurl)
        iid = re.search(r'iid\s*[:=]\s*(\d+)', response).group(1)
        title = re.search(r"kw\s*[:=]\s*'([^']+)'", response.decode('gb18030')).group(1)
        _, _, _, response = fetch('http://v2.tudou.com/v?it=' + iid + '&st=1,2,3,4,99')
        doc = parseString(response)
        title = title or doc.firstChild.getAttribute('tt') or doc.firstChild.getAttribute('title')
        urls = [(int(n.getAttribute('brt')), n.firstChild.nodeValue.strip()) for n in doc.getElementsByTagName('f')]
        url = max(urls, key=lambda x:x[0])[1]
        print url
        if len(urls) == 1:
            url = urls[0]
            res.append(['', url])
        else:
            for url in urls:
                res.append(['', url[1]])
        return res
    except:
        t, v, tb = sys.exc_info()
        log.error("GetTuDouRealUrl playurl:  %s, %s,%s,%s" % (playurl, t, v, traceback.format_tb(tb)))
        return GetTuDouRealUrl(playurl, times + 1)

def GetRealPlayUrl(playurl, times=0):
    res = []
    if times > MAX_TRY:
        return res
    _, netloc, _, _, _, _ = urlparse(playurl)
    if netloc == BAIY_HOST:
        result = GetBaiyRealUrl(playurl)
        for _, _, lang, realurl, _ in result:
            lang = urllib.unquote(lang).decode("utf-8").replace('%', '\\').decode('unicode_escape').encode('utf-8')
            res.append([lang, realurl])
    elif netloc == AIPAI_HOST:
        result = GetAiPaiRealUrl(playurl)
        res.extend(result)
    elif netloc == WL_HOST:
        result = GetWlRealUrl(playurl)
        res.extend(result)
    elif netloc == UK_HOST:
        result = GetUKouRealUrl(playurl)
        res.extend(result)
    elif netloc == SOHU_HOST:
        result = GetSoHuRealUrl(playurl)
        res.extend(result)
    elif netloc == KUSIX_HOST:
        result = GetKuSixRealUrl(playurl)
        res.extend(result)
    elif netloc == TUDOU_HOST:
        result = GetTuDouRealUrl(playurl)
        res.extend(result)
    
    return res
            

class Job(object):
    
    mainurl = None
    purl = None
    supporturl = None
    
    rating = None
    title = None
    url = None
    source = None
    area = None
    actor = None
    cid = None
    duration = None
    intro = None
    s_intro = None
    date = None
    ctype = None
    imgh_url = None
    imgv_url = None
    real_url = None
    url_type = None
    
    anchor = None
    playtimes = None
    reallinks = []
    

    def __init__(self, **kwargs):
        self.__dict__ = kwargs

class Worker(threading.Thread):

    def __init__(self, job_queue):
        threading.Thread.__init__(self)
        self.job_queue = job_queue
        self.ods = DataSaver()
        self.sfs = DataSaver()

    def run(self):
        tname = threading.current_thread().getName()
        ods_policy = {'roll_policy':'time:hour', 'namefmt':'data%/%t/%t/res_%t.dat', 'timefmt':['%Y', '%Y%m%d', '%Y%m%d_%H%M%S']}
        self.ods.set_filename_format(ods_policy)
        while True:
            try:
                job = self.job_queue.get()
                log.info('From: %s, fetch %s , %s, start.' % (tname, job.url_type, job.url))
                if job.url_type == LIST_URL_TYPE:
                    result = GetListData(job.url)
                    for rating, title, url, source, area, actor, cid, duration, intro, s_intro, date, ctype, imgh_url, imgv_url in result:
                        purl = BASE_URL % int(cid)
                        supporturl = SUPPORT_URL % (urllib.quote(title.encode('gb18030')))
                        newjob = Job(rating=rating, title=title, url=purl, supporturl=supporturl, mainurl=url, source=source, area=area, actor=actor, cid=cid, \
                                     duration=duration, intro=intro, s_intro=s_intro, date=date, ctype=ctype, imgh_url=imgh_url, imgv_url=imgv_url, url_type=ITEM_URL_TYPE)
                        self.job_queue.put(newjob)
                elif job.url_type == ITEM_URL_TYPE:
                    result = GetItemData(job.url, 1)
                    if not result :
                        result = GetItemData(job.supporturl, 2)
                    for playurl, anchor, playtimes in result:
                        newjob = Job(rating=job.rating, title=job.title, purl=job.purl, supporturl=job.supporturl, mainurl=job.mainurl, source=job.source, area=job.area, actor=job.actor, cid=job.cid, \
                                     duration=job.duration, intro=job.intro, s_intro=job.s_intro, date=job.date, ctype=job.ctype, imgh_url=job.imgh_url, imgv_url=job.imgv_url, url_type=REAL_URL_TYPE, \
                                     url=playurl, anchor=anchor, playtimes=playtimes)
                        self.job_queue.put(newjob)
                elif job.url_type == REAL_URL_TYPE:
                    result = GetRealPlayUrl(job.url)
                    job.reallinks = json.dumps(result, ensure_ascii=False, encoding='utf-8')
                    log.error(job.reallinks)
                    self.process_job(job)
                
                sleep(random.uniform(10, 11.8))
            except:
                t, v, tb = sys.exc_info()
                log.error("url:  %s %s,%s,%s" % (job.url, t, v, traceback.format_tb(tb)))

    def process_job(self, job):
        newtaskdata = json.dumps(job.__dict__, ensure_ascii=False, encoding='utf-8')
        self.ods.save_data("%s\n" % newtaskdata, no_head=True)
        
        
class Crawler(object):
    
    def __init__(self, limit):
        self.workers = []
        self.limit = limit
        self.job_queue = PriorityQueue()
        
        
    def start(self, seed_path):
        count = 0
        checkfile = open('count', 'r')
        lines = checkfile.readlines()
        if len(lines) > 0 and len(lines[-1].strip()) > 1:
            count = int(lines[-1].strip())
            count = count - self.limit * 2
        checkfile.close()
        
        log.info('init_count: %d' % count)
        
        seed_file = open(seed_path, 'r')
        checkfile = open('count', 'w')
        
        for _ in range(count):
            seed_file.readline()
            
        runnable_threshold = 10
        for _ in range(self.limit):
            worker = Worker(self.job_queue)
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

        while True and runnable_threshold > 0:
            log.info('current queue size: %s' % str(self.job_queue.qsize()))
            if self.job_queue.qsize() < self.limit:
                for _ in range(self.limit):
                    count += 1
                    url = seed_file.readline().strip('\r\n')
                    
                    if not url:
                        log.info("no shop url left")
                        runnable_threshold -= 1
                        break
                    job = Job(url=url, url_type=LIST_URL_TYPE)
                    self.job_queue.put(job)
                checkfile.write(str(count) + '\n')
                checkfile.flush()
                log.info('new_count: %d.' % count)
            time.sleep(5)
            

def main():
    #Crawler(10).start('ct')
    if len(sys.argv) == 3:
        Crawler(int(sys.argv[1])).start(sys.argv[2])
    else:
        print "example: %s 5 c1" % sys.argv[0]

def test():
#     print GetUKouRealUrl('http://v.youku.com/v_show/id_XNTg1OTI5NTQ0.html')
#     print GetUKouRealUrl('http://v.youku.com/v_show/id_XNTgzNTE2OTcy.html')
#     print GetSoHuRealUrl('http://tv.sohu.com/20120111/n331899511.shtml')
#     print GetKuSixRealUrl('http://v.ku6.com/show/c3UTb-y6XRScUY5UsxOLlQ...html')
#     print GetWlRealUrl('http://www.56.com/u73/v_OTI2NzQ1MzQ.html')
#     print GetWlRealUrl('http://www.56.com/w28/play_album-aid-11265281_vid-OTEwMDA2Nzc.html')
#     print GetRealPlayUrl('http://www.baiy.net/player_2009/55432/55432-0-0.html')
    print GetTuDouRealUrl('http://www.tudou.com/programs/view/it884YETOlM/')

if __name__ == "__main__":
    pass
    main()
    #test()

        
        
