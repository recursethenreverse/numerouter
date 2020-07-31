import json
import time
import sys

from ad_detect import DetectAds
from rmq import RMQiface

from urllib.request import urlopen


def is_ok_image(url: str) -> bool:
    try:
        site = urlopen(url)
        meta = site.info()  # get header of the http request
        return int(meta['content-length']) > 10000
    except Exception:
        return False

da = DetectAds([
    'https://raw.githubusercontent.com/hectorm/hmirror/master/data/adaway.org/list.txt',
    'https://raw.githubusercontent.com/hectorm/hmirror/master/data/adblock-nocoin-list/list.txt',
    'https://raw.githubusercontent.com/hectorm/hmirror/master/data/easylist/list.txt',
    'https://raw.githubusercontent.com/StevenBlack/hosts/master/hosts',
])

if __name__ == '__main__':
    loop = True
    img = False

    for arg in sys.argv[1:]:
        if arg == '-once':
            loop = False
        if arg == '-img':
            img = True


    with open('reader_config.json', 'r') as f:
        config = json.load(f)

    host = config['host']
    usr = config['user']
    pwd = config['password']

    # Read log lines parse and write if ad
    if not img:
        in_queue = config['queue_name']
        out_queue = config['filtered_queue_name']
        mq_reader = RMQiface(host, in_queue, usr, pwd)
        mq_writer = RMQiface(host, out_queue, usr, pwd)
        while True:
            line = mq_reader.read()
            if not line:
                time.sleep(1.0)
                if loop:
                    continue
                else:
                    break
            fields = line.split(' ||| ')
            if da.check(fields[3]):
                mq_writer.write(fields[3])

    # Read urls and write if image and of sufficient size
    else:
        in_queue = config['filtered_queue_name']
        out_queue = config['filtered_images_queue_name']
        mq_reader = RMQiface(host, in_queue, usr, pwd)
        mq_writer = RMQiface(host, out_queue, usr, pwd)
        while True:
            line = mq_reader.read()
            if not line:
                time.sleep(1.0)
                if loop:
                    continue
                else:
                    break
            if is_ok_image(line):
                mq_writer.write(line)
