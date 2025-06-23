import json
import logging
import datetime

import pandas as pd
import os
from classes.google_drive_handler import GoogleDriveHandler


def convert_to_csv_and_upload():
    logging.info("Converting JSONL to CSV")
    try:
        with open("./data/magalu_feed.jsonl", "r", encoding="utf-8") as jsonl_file:
            precos = []

            for line in jsonl_file:
                data = json.loads(line)
                precos.append(
                    {
                        "ean": data.get("ean"),
                        "sku_casas_bahia": data.get("sku_casas_bahia"),
                        "sku_concorrente": data.get("sku_concorrente"),
                        "site_concorrente": data.get("site_concorrente"),
                        "codigo_site": data.get("codigo_site"),
                        "vendido_por": data.get("vendido_por"),
                        "preco_a_prazo": data.get("preco_a_prazo"),
                        "preco_a_vista": data.get("preco_a_vista"),
                        "preco_exclusivo": data.get("preco_exclusivo"),
                        "data": data.get("data"),
                        "hora": data.get("hora"),
                        "estoque": data.get("estoque"),
                        "qdt_parcelas_preco_sugerido_site": data.get("qdt_parcelas_preco_sugerido_site"),
                        "valor_parcelas_preco_sugerido_site": data.get("valor_parcelas_preco_sugerido_site"),
                        "taxa_juros_preco_sugerido_site": data.get("taxa_juros_preco_sugerido_site"),
                        "qtde_max_parcelas_cartao_bandeirado": data.get("qtde_max_parcelas_cartao_bandeirado"),
                        "valor_parcelas_cartao_bandeirado": data.get("valor_parcelas_cartao_bandeirado"),
                        "taxa_juros_cartao_bandeirado": data.get("taxa_juros_cartao_bandeirado"),
                        "qtde_max_parcelas_cartao_proprio": data.get("qtde_max_parcelas_cartao_proprio"),
                        "valor_parcelas_cartao_proprio": data.get("valor_parcelas_cartao_proprio"),
                        "taxa_juros_cartao_proprio": data.get("taxa_juros_cartao_proprio"),
                        "cep": data.get("cep"),
                        "cidade": data.get("cidade"),
                        "tipo_frete": data.get("tipo_frete"),
                        "valor_frete": data.get("valor_frete"),
                        "prazo_entrega": data.get("prazo_entrega"),
                        "url": data.get("url"),
                    }
                )

        current_time_utc = datetime.datetime.utcnow()
        brazil_time = current_time_utc - datetime.timedelta(hours=3)
        now = brazil_time.isoformat()[:19].replace(":", "-")

        filename_precos = f"./data/MAGALU_{now}.csv"

        if precos:
            df_precos = pd.DataFrame(precos)
            df_precos.to_csv(filename_precos, index=False, encoding="utf-8")
            logging.info(f"Saved prices to {filename_precos}")

        drive_handler = GoogleDriveHandler("credentials.json", "drive_token.pickle")
        drive_folder_id = os.getenv("DRIVE_FOLDER_ID")

        if os.path.exists(filename_precos):
            drive_handler.upload_file(filename_precos, os.path.basename(filename_precos), drive_folder_id)
            logging.info(f"Uploaded {filename_precos} to Google Drive")

    except Exception as e:
        logging.error(f"Error converting JSONL to CSV: {str(e)}")
