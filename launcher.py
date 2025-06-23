import logging
import os
import urllib3

from extract import extract

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logging.disable(logging.DEBUG)

phase = os.getenv("PHASE")

if __name__ == '__main__':
    if phase == "EXTRACT":
        extract()
