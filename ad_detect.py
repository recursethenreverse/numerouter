import sys
import logging
import os
from urllib.request import urlretrieve
import hashlib

from typing import List, Set


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler(sys.stderr))


class DetectAds:
    def __init__(self, blacklist_urls: List[str]):
        itemset = self.load_blacklists(blacklist_urls)
        self.filter = itemset

    def load_blacklists(self, blacklist_urls: List[str]) -> Set[str]:
        """ takes list of blacklist urls """
        items: Set[str] = set()
        sets = []
        for url in blacklist_urls:
            fname = f'/tmp/{hashlib.md5(url.encode()).hexdigest()}-blacklist'
            # Download if necessary
            if not os.path.exists(fname):
                log.debug(f'Downloading {url} -> {fname}')
                urlretrieve(url, fname)

            fset = set(line.strip() for line in open(fname))
            log.debug(f'Got {len(fset)} records from \t {url}')
            sets.append(fset)

        log.debug(f'Total: {sum(len(s) for s in sets)} records')
        items = items.union(*sets)
        log.debug(f'Aggregated into {len(items)} records')
        return items

    def check(self, url: str) -> bool:
        ss = url.find('//') + 2
        ss = ss if ss != 1 else 0  # not found
        return url[ss:url.find('/', ss)] in self.filter
