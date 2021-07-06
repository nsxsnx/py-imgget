#!/usr/bin/python3
import os, urllib.request, re, urllib.parse, urllib.error, shutil, io, operator, json, threading, sys, sqlite3
from PIL import Image
from datetime import datetime
from time import sleep

# Source:
URL = 'www.zzzzz.com/'
URL_SUFFIX = '?page=%d'
PAGES = range(1, 150)

# General options:
VERBOSE = False
BASE_DIR = '/u01/upload'
DESCR_FILE_NAME = 'info.dsc'
MAX_SIMULTANEOUS_THREADS = 5
MAX_SEQUENTIAL_ERRORS = 5
DOWNLOAD_LIMIT = 1500
SLEEP_BETWEEN_URLS = 2
DB_NAME = '/u01/scripts/imgget/db/imgget.db'
REQUEST_HEADERS = {        # Referer will be aded automatically
        'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0',
        }

# Image filters:
DESCR_MIN_LENGTH = 10
SIZE_MIN_SUM = 1100        # minimal sum of width and height
ACCEPT_PIXEL_MODES = [ 'RGB', 'RGBA']
EXCLUDE_DESCR = (
        'http', 
        'www',
        ' com',            # dots deleted at normilize_str
        'dot-com',
        )
EXCLUDE_TAGS = (
        )
EXCLUDE_PAGEURL = (
        '/video/',
        )
EXCLUDE_IMGTYPES = (
        '.gif',
        )

# Image processing:
DELETE_PATTERNS = (        # regexp to search in Descr and Tags
        '#? ?\d{5,}',      # pin number
        '\.',              # don't delete this: 
                           # can be in tags, 
                           # relied on in STOPLIST_DESCR
        '(^| )pin( |$)',   #
        ' +',              # remove repeting whitespaces
        )

# Image border:
BORDER_COLOR = (246, 246, 246)
BORDER_HEIGHT = 24
K_DIFF = 10

RE_LINK = """\<a\                          # <a_    
            (?:(?!<\/a\>).)*               # not capturing not </a>
            href="(?P<href>/[^"]+)"        # href tag, starting with /, at least two chars
            (?:(?!<\/a\>).)*               # not capturing not </a>
            \<img\                         # <img_
            .*?                            # any non-greedy
            \<\/a\>                        # </a> """
RE_METADESCR = """\<meta\ name=\"description\"\ content=\"
                    .*?
                    (Description:(?P<description>.+?))?
                    (Tags:(?P<tags>.+?))?
                    \"\ \/\>"""
RE_METAIMG = "\<div\ class=\"image_frame\"\ .*?\<img\ .*?src=\"(?P<image>.+?)\""
RE_URL_PIN = '/(\d{4,})-?'


class sqlite_pinstorage:
    _conn, _c, _counter, commit_limit = None, None, 0, 0
    _counter_lock = threading.Lock()
    def __init__(self, db_name, commit_limit = 100):
        self._commit_limit = commit_limit
        self._conn = sqlite3.connect(db_name, detect_types=sqlite3.PARSE_DECLTYPES)
        self._c = self._conn.cursor()
        self._c.execute('''CREATE TABLE IF NOT EXISTS pins
             (id INTEGER PRIMARY KEY,
              pin INTEGER UNIQUE NOT NULL,
              timestamp timestamp)''')
        self._c.execute('CREATE UNIQUE INDEX IF NOT EXISTS pin_index on pins (pin)')
    def exists(self, pin):
        t = (pin, )
        self._c.execute('SELECT * FROM pins where pin=?', t)
        if self._c.fetchone() is not None: return True
        return False
    def add(self, pin):
        t = (pin, datetime.now())
        self._c.execute('INSERT INTO pins VALUES (NULL, ?, ?)', t)
        with self._counter_lock: 
            self._counter += 1
            if self._counter >= self._commit_limit: 
                self._conn.commit()
                self._counter = 0
    def __del__(self):
        self._conn.commit()
        self._conn.close()

def crop_image(thread_num, im, depth = 0):
    def is_similar_color(a, b):
        return sum(tuple(map(abs, tuple(map(operator.sub, a, b))))) <= K_DIFF

    def has_border(pixs):
        return len([p for p in pixs if is_similar_color(BORDER_COLOR, p)]) >= len(pixs)*2/3

    if depth >= 2: return im
    pixs = [
            im.getpixel( (0, im.size[1]-1) ),
            im.getpixel( (im.size[0]/2, im.size[1]-1) ),
            im.getpixel( (im.size[0] - 1, im.size[1]-1) ),
           ]
    if VERBOSE: print('%02d: Border: %s' % (thread_num, pixs))
    if has_border(pixs):
        if VERBOSE: print('%02d: Cropping border' % (thread_num,))
        im = im.crop((0, 0, im.size[0], im.size[1] - BORDER_HEIGHT))
        im = crop_image(thread_num, im, depth + 1) # there can be nested borders
    return im

def test_page(thread_num, img):
    if len(img['descr']) < DESCR_MIN_LENGTH: return False
    if any([s.lower() in img['descr'].lower() for s in EXCLUDE_DESCR]): return False
    if any([img['url'].endswith(s) for s in EXCLUDE_IMGTYPES]): return False
    if any([s in img['page_url'] for s in EXCLUDE_PAGEURL]): return False
    tags = [tag.strip() for tag in re.split(',', img['tags'])]
    if any([s in tags for s in EXCLUDE_TAGS]): return False
    return True

def test_img(thread_num, im):
    if im.mode not in ACCEPT_PIXEL_MODES: return False
    if im.size[0] + im.size[1] < SIZE_MIN_SUM: return False
    return True

def normalize_str(s):
    for expr in DELETE_PATTERNS: s = re.sub(expr, ' ', s, flags = re.IGNORECASE)
    return s.strip()

def process_page(thread_num, page_url):
    print('%02d: Requesting url: %s' % (thread_num, page_url))
    req = urllib.request.Request(page_url, headers = REQUEST_HEADERS)
    try: resp = urllib.request.urlopen(req)
    except urllib.error.URLError as e: 
        if hasattr(e, 'reason'): print('%02d: Network error: %s' % (thread_num, e.reason))
        elif hasattr(e, 'code'): print('%02d: Server error: %s' % (thread_num, e.code))
        return None
    page = str(resp.read())
    img = { 'page_url': page_url }
    match = re.search(RE_METADESCR, page, re.DOTALL|re.VERBOSE)
    if match is None: return None
    #try:    img["pin"] = match.group('pin')
    #except: img["pin"] = ''
    try:    img["descr"] = normalize_str(match.group('description'))
    except: img["descr"] = ''
    try:    img["tags"] = normalize_str(match.group('tags'))
    except: img["tags"] = ''

    match = re.search(RE_METAIMG, page, re.DOTALL|re.VERBOSE)
    if match is None: return None
    try: img["url"] = match.group('image')
    except: return None

    if VERBOSE:
        for k,v in sorted(img.items()):
            print('%02d: %s: %s' % (thread_num, k, v))
    return img

def get_page(thread_num, url):
    try: pin = re.search(RE_URL_PIN, url).group(1)
    except: return 1
    target_dir = os.path.join(BASE_DIR, pin)
    #if os.path.exists(target_dir) and os.listdir(target_dir):
    pindb = sqlite_pinstorage(DB_NAME)
    if pindb.exists(pin):
        print('%02d: Already downloaded, skipping' % (thread_num,))
        return 1
    img = process_page(thread_num, url)
    if not img:
        print('%02d: Error processing page, skipping' % (thread_num,))
        return 1
    if not test_page(thread_num, img): 
        print('%02d: Quality tests (page) failed, skipping' % (thread_num,))
        pindb.add(pin)
        return 1
    print('%02d: Getting file: %s' % (thread_num, img['url']))
    req = urllib.request.Request(img['url'], headers = REQUEST_HEADERS)
    try: resp = urllib.request.urlopen(req)
    except urllib.error.URLError as e: 
        if hasattr(e, 'reason'): print('%02d: Network error: %s' % (thread_num, e.reason))
        elif hasattr(e, 'code'): print('%02d: Server error: %s' % (thread_num,  e.code))
        return 1
    filename = os.path.basename(urllib.parse.urlparse(img['url']).path)
    try: filename = filename.split('-',1)[1]
    except: 
        if not len(filename): return 1
        pass
    data = resp.read()
    if len(data) < int(resp.headers['Content-Length']):
        print('%02d: Downloading error: content too short' % (thread_num,))
        return 1
    im = Image.open(io.BytesIO(data))
    if not test_img(thread_num, im): 
        print('%02d: Quality (image) tests failed, skipping' % (thread_num,))
        pindb.add(pin)
        return 1
    im = crop_image(thread_num, im)
    if not os.path.exists(target_dir): os.mkdir(target_dir, 0o750)
    target_full = os.path.join(target_dir, filename)
    print('%02d: Saving to %s' % (thread_num, target_full))
    try: im.save(target_full, progressive=True)
    except: 
        print('%02d: File writing error: %s' % (thread_num, target_full))
        shutil.rmtree(target_dir)
        return 1
    target_full = os.path.join(target_dir, DESCR_FILE_NAME)
    with open(target_full, 'w') as f:
        try: f.write(json.dumps(img))
        except:
            print('%02d: File writing error: %s' % (thread_num, target_full))
            shutil.rmtree(target_dir)
            return 1
    pindb.add(pin)
    return 0

def get_page_wrapper(thread_num, url):
    global errnum
    with sem:
        if get_page(thread_num, url):
            with errnum_lock: errnum += 1

def main(URL):
    global errnum
    global dimgnum
    errnum = 0
    url_suffix = URL_SUFFIX
    if URL.find('?') >= 0: url_suffix = url_suffix.replace('?', '&')
    if URL.endswith('/') and url_suffix.startswith('/'): url_suffix = url_suffix[1:]
    URL = URL + url_suffix
    if not URL.startswith('http://www.') and not URL.startswith('www.'): URL = 'www.' + URL
    if not URL.startswith('http://'): URL = 'http://' + URL
    uparsed = urllib.parse.urlparse(URL)
    BASE_URL = uparsed.scheme + '://' + uparsed.netloc
    REQUEST_HEADERS['Referer'] = BASE_URL

    pages_err_cnt = 0
    for curpage in PAGES:
        cururl = URL % curpage
        print('Requesting url:', cururl)
        req = urllib.request.Request(cururl, headers = REQUEST_HEADERS)
        try: resp = urllib.request.urlopen(req)
        except urllib.error.URLError as e: 
            if hasattr(e, 'reason'): print('Network error:', e.reason)
            elif hasattr(e, 'code'): print('Server error: ', e.code)
            pages_err_cnt += 1
            if pages_err_cnt >= MAX_SEQUENTIAL_ERRORS: 
                print('Too many errors, giving up on this url')
                return 
            continue
        pages_err_cnt = 0
        data = str(resp.read())
        matches = re.findall(RE_LINK, data, re.DOTALL|re.VERBOSE)
        mnum = len(matches)
        print(mnum, 'sublinks found')
        errnum = 0
        threads = []
        for cnt, item in enumerate(matches):
            t = threading.Thread(target = get_page_wrapper, args = (cnt, BASE_URL + item), name = cnt)
            t.start()
            threads.append(t)
        for t in threads: t.join()
        print('\n%d of %d pages processed successfully from %s\n' % (mnum-errnum, mnum, cururl)) 
        with dimgnum_lock: dimgnum += mnum-errnum
        if DOWNLOAD_LIMIT and dimgnum > DOWNLOAD_LIMIT: 
            print ('DOWNLOAD_LIMIT reached, no more pages of current url')
            break
        if SLEEP_BETWEEN_URLS: sleep(SLEEP_BETWEEN_URLS)

# ENTRY POINT
sem = threading.BoundedSemaphore(value=MAX_SIMULTANEOUS_THREADS)
errnum_lock = threading.Lock()
errnum = 0
dimgnum_lock = threading.Lock()
dimgnum = 0

URL_FILE = None
if len(sys.argv) > 1:
    if sys.argv[1].startswith('http://') or sys.argv[1].startswith('www.') or not os.path.exists(sys.argv[1]): URL = sys.argv[1]
    else: URL_FILE = sys.argv[1]
if not os.path.exists(BASE_DIR): os.makedirs(BASE_DIR, 0o750)
if URL_FILE:
    with open(URL_FILE) as f:
        urls_list = [l.split('#', 1)[0].strip() for l in f.readlines()]
        urls_list = [l for l in urls_list if len(l)]
    for u in urls_list: 
        if DOWNLOAD_LIMIT and dimgnum > DOWNLOAD_LIMIT: 
            print ('DOWNLOAD_LIMIT reached, no more urls of current list, exiting')
            exit()
        else: main(u)
else: main(URL)
