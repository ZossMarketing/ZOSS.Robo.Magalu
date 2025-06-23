import json
import time
import logging
import random
from classes.magalu_request_solver import MagazineLuizaScraper
from concurrent.futures import ThreadPoolExecutor, as_completed
from functions import helpers


MAX_WORKERS = 50
MAX_RETRIES = 3
RETRY_DELAY = (2, 4)


def process_request(cep_item, ean_item, attempt=1):
    cep = cep_item["cep"]
    city = cep_item["cidade"]
    ean = ean_item["EAN"]
    sku_on = ean_item["SKU_ON"]

    logging.info(f"[Tentativa {attempt}] Processando CEP: {cep} ({city}) - EAN: {ean}")

    try:
        scraper = MagazineLuizaScraper(cep, ean, city, sku_on)
        scraper.process_products()
        return {"success": True, "cep": cep, "ean": ean}
    except Exception as e:
        logging.error(f"[Tentativa {attempt}] Erro ao processar CEP {cep} e EAN {ean}: {e}")

        if attempt < MAX_RETRIES:
            time.sleep(random.uniform(*RETRY_DELAY))
            return process_request(cep_item, ean_item, attempt + 1)
        else:
            return {"success": False, "cep": cep, "ean": ean, "error": str(e)}


def run(cep_data, ean_data):
    tasks = [(cep, ean) for cep in cep_data for ean in ean_data]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for cep_item, ean_item in tasks:
            time.sleep(random.uniform(1.3, 2))
            futures.append(executor.submit(process_request, cep_item, ean_item))

        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logging.error(f"Erro no future: {e}")

    # Estatísticas
    successful = sum(1 for r in results if r.get("success"))
    failed = len(results) - successful

    logging.info(f"Extração concluída: {successful} sucessos, {failed} falhas")

    try:
        with open("./data/magalu_feed.jsonl", "r", encoding="utf-8") as f:
            total_lines = sum(1 for _ in f)
        logging.info(f"Volume de itens processados: {total_lines}")
    except FileNotFoundError:
        logging.warning("Arquivo não encontrado")


def extract():
    with open("./files/ceps.json", "r", encoding="utf-8") as f:
        cep_data = json.load(f)

    with open("./files/eans.json", "r", encoding="utf-8") as f:
        ean_data = json.load(f)

    run(cep_data, ean_data)

    helpers.convert_to_csv_and_upload()
