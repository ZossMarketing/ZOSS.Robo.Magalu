import re
import json
import time
import logging
import random
from datetime import datetime
import requests
import cloudscraper


class MagazineLuizaAPI:
    def __init__(self, headers_busca, cookies_busca, headers_produto, headers_frete):
        self.headers_busca = headers_busca
        self.cookies_busca = cookies_busca
        self.headers_produto = headers_produto
        self.headers_frete = headers_frete
        self.url_frete = "https://federation.magazineluiza.com.br/graphql"
        self.proxies = {
            "http": "http://brd-customer-hl_1c890fdd-zone-movida_mensal-country-br:i0jhk3a1o8cv@brd.superproxy.io:33335",
            "https": "http://brd-customer-hl_1c890fdd-zone-movida_mensal-country-br:i0jhk3a1o8cv@brd.superproxy.io:33335",
        }

    def search_products(self, search_url, max_retries=5):
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    search_url,
                    headers=self.headers_busca,
                    cookies=self.cookies_busca,
                    proxies=self.proxies,
                    timeout=15,
                )
                next_data = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', response.text)

                if next_data:
                    data = json.loads(next_data.group(1))
                    return data.get("props", {}).get("pageProps", {}).get("data", {}).get("search", {})
                return None
            except Exception as e:
                if attempt < max_retries - 1:
                    # logging.warning(f"Error on attempt {attempt + 1}, retrying...")
                    time.sleep(random.uniform(3, 7))
                    continue
                logging.error(f"Error searching products: {e}")
                return None

    def get_product_details(self, url_produto, max_retries=5):
        for attempt in range(max_retries):
            try:
                headers = self.headers_produto.copy()
                headers["referer"] = url_produto

                response = requests.get(
                    url_produto,
                    headers=headers,
                    proxies=self.proxies,
                    timeout=15,
                )
                next_data = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', response.text)

                if next_data:
                    return json.loads(next_data.group(1))
                return None
            except Exception as e:
                if attempt < max_retries - 1:
                    # logging.warning(f"Error on attempt {attempt + 1}, retrying...")
                    time.sleep(random.uniform(3, 7))
                    continue
                logging.error(f"Error getting product details: {e}")
                return None

    def get_shipping(self, payload, max_retries=5):
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    self.url_frete,
                    headers=self.headers_frete,
                    json=payload,
                    proxies=self.proxies,
                    timeout=15,
                )
                return response.json()
            except Exception as e:
                if attempt < max_retries - 1:
                    # logging.warning(f"Error on attempt {attempt + 1}, retrying...")
                    time.sleep(random.uniform(3, 7))
                    continue
                logging.error(f"Error getting shipping: {e}")
                return None


class MagazineLuizaScraper:
    def __init__(self, cep, search_term, city=None, sku_casas_bahia=None):
        self.cep = cep
        self.search_term = search_term
        self.city = city
        self.sku_casas_bahia = sku_casas_bahia

        # Lista de User Agents do Chrome Windows
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        ]

        # Seleciona um UA aleatório para esta instância
        self.current_ua = random.choice(self.user_agents)

        self.search_url = f"https://www.magazineluiza.com.br/busca/{search_term}/"
        self.processed_count = 0
        self.results = []
        self.shield_cookies = self._get_shield_square_cookies()

        # Initialize API with original headers/cookies
        self.api = MagazineLuizaAPI(
            headers_busca=self._get_search_headers(),
            cookies_busca=self._get_search_cookies(),
            headers_produto=self._get_product_headers(),
            headers_frete=self._get_shipping_headers(),
        )

    def _get_current_user_agent(self):
        """Rotaciona o user agent a cada chamada"""
        self.current_ua = random.choice(self.user_agents)
        return self.current_ua

    def _get_shield_square_cookies(self, max_retries=5):
        for attempt in range(max_retries):
            scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "linux", "desktop": True})

            scraper.proxies = {
                "http": "http://brd-customer-hl_1c890fdd-zone-movida_mensal-country-br:i0jhk3a1o8cv@brd.superproxy.io:33335",
                "https": "http://brd-customer-hl_1c890fdd-zone-movida_mensal-country-br:i0jhk3a1o8cv@brd.superproxy.io:33335",
            }

            scraper.get(f"https://www.magazineluiza.com.br/busca/{self.search_term}/")
            # logging.info(url.url)
            time.sleep(random.uniform(3, 7))
            cookies = {k: v for k, v in scraper.cookies.get_dict().items() if "__uz" in k}
            # logging.info(f"Shield cookies capturados: {cookies}")

            if cookies:
                return cookies

            logging.info(f"Tentativa {attempt + 1} sem cookies, retentando...")

        return {}

    def _get_search_headers(self):
        return {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "referer": "https://validate.perfdrive.com/",
            "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": self._get_current_user_agent(),
        }

    def _get_search_cookies(self):
        cookies = {
            "noe_freight": "AUTO",
            "noe_hub_shipping_enabled": "1",
            "toggle_wishlist": "false",
            "FCCDCF": "1",
            "ml2_redirect_8020": "0",
            "FCNEC": "1",
            "mixer_shipping": "AUTO",
            "mixer_hub_shipping": "true",
            "toggle_pdp_seller_score": "true",
            "toggle_vwo": "true",
            "toggle_agatha": "true",
            "toggle_ads": "true",
            "toggle_new_service_page": "true",
            "toggle_quick_click": "false",
            "enable_fallback_banner": "0",
            "MLPARCEIRO": "71815",
            "__rtbh.lid": '{"eventType":"lid","id":"HXSJjlFmBDP6R6Xu4Gyw","expiryDate":"2026-06-06T14:54:43.005Z"}',
            "__ssds": "3",
            "__spdt": "9d1e373ebf72458681aa6b13c59c355a",
            "__ssuzjsr3": "a9be0cd8e",
            "__uzmaj3": "8dab4c30-0a57-43e5-ba5a-71e67b57e34a",
            "__uzmbj3": "1749221683",
            "__uzmcj3": "137511087512",
            "__uzmdj3": "1749221683",
            "_vwo_uuid_v2": "D6B8D2471EC61450D3201A16325DD8D1D|6d0d9ce65ec592e68e8145726e4d4989",
            "_tt_enable_cookie": "1",
            "_ttp": "01JX2VSBYQRH296D05AWF8A3D9_.tt.2",
            "ttcsid": "1749221683162::CZb525mUMkLCpff9PkBy.1.1749221683162",
            "_vwo_uuid": "D6B8D2471EC61450D3201A16325DD8D1D",
            "_vwo_ds": "3$1749221683:72.07378558:::",
            "_vwo_sn": "0:1:::1",
            "_vis_opt_s": "1|",
            "_vis_opt_test_cookie": "1",
            "_gcl_au": "1.1.1325721257.1749221683",
            "ttcsid_C1I87V1T0U322RQPSRKG": "1749221683162::IHqUkd1gc6nUf_DV8eIK.1.1749221683380",
            "_fbp": "fb.2.1749221683392.728223087395768917",
            "_hjSessionUser_4936838": "eyJpZCI6ImZlZGZiN2I2LWI1ZWYtNTZiMy1hNTA3LTM2ZDMwNWI4YjYyYiIsImNyZWF0ZWQiOjE3NDkyMjE2ODM0NTgsImV4aXN0aW5nIjpmYWxzZX0=",
            "_hjSession_4936838": "eyJpZCI6ImJkNDI0OGI1LWM3YjAtNDZmYy1iMmEzLTcyMmVhNzU1NTM4NiIsImMiOjE3NDkyMjE2ODM0NTgsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MX0=",
            "_clck": "xc8mrq|2|fwj|0|1983",
            "_pin_unauth": "dWlkPU1qazRaR0ZsT0RZdFptSTRZUzAwTWpNekxXSXpOakV0TTJJMU56UmxNR0V4TVdSaA",
            "ml_tid": "f11b90c8-0ebf-47f5-be5f-e10ba16d9690",
            "stwu": "temp_8b143557-ec91-45f7-b320-b587e0c5c68d",
            "stwt": "1",
            "__bid": "deb14bee-6d59-4d6b-b1b0-bc91bfd78ea1",
            "_azfp_sc": "84a4d962edb3a90f6c367438f38e7e70d448a02463971b96d147cce2f31137af",
            "cto_bundle": "Kql-EV9BVmhNd1BYTVBna0NreVdseGc2U3RpaEYyRUFIZVNQTEdtdDFFRUhJZDlweGFtSHZHMlhEMEx6dmo1elhTN1BqWXJndW9VeWR6OEpES2JYRnBVWkRWRnRjOXI0em9YYlR6dEtQR1NFN3h1WFUxcVhSMDBVSWZCbHJaV012MU9kQWM3RkpKWnpuYXo4Wm82MmRVdllYZkElM0QlM0Q",
            "_ga": "GA1.1.374703252.1749221684",
            "_clsk": "lo2zm6|1749221684670|1|0|j.clarity.ms/collect",
            "storedSessionIdLCJ5VBTH8V": "s1749221684$o1$g1$t1749221684$j60$l0$h0",
            "GTMUtmTimestamp": "1749221684807",
            "GTMUtmSource": "(direct)",
            "GTMUtmMedium": "(none)",
            "GTMIsTrueDirect": "1",
            "__privaci_cookie_consent_uuid": "5974480b-81db-459a-818f-065069a9f97a:22",
            "__privaci_cookie_consent_generated": "5974480b-81db-459a-818f-065069a9f97a:22",
            "__privaci_cookie_consents": '{"consents":{"1":1,"2":1,"3":1,"4":1},"location":"RJ#BR","lang":"pt","gpcInBrowserOnConsent":false,"gpcStatusInPortalOnConsent":false,"status":"record-consent-success","implicit_consent":true}',
            "__privaci_latest_published_version": "18",
            "zipcode": "23040150",
            "zipcode_checksum": "MjMwNDAxNTA6UmlvIGRlIEphbmVpcm86Uko=",
            "zipcode_city": "Rio%20de%20Janeiro",
            "zipcode_state": "RJ",
            "zipcode_latitude": "-22.9270813",
            "zipcode_longitude": "-43.553688",
            "__rtbh.uid": '{"eventType":"uid","id":"unknown","expiryDate":"2026-06-06T14:55:59.369Z"}',
            "_uetsid": "2e2d8a1042e611f0874587984b464aed",
            "_uetvid": "2e2db12042e611f083930deba9eed671",
            "_dd_s": "rum=0&expire=1749222892292",
            "_ga_LCJ5VBTH8V": "GS2.1.s1749221684$o1$g1$t1749221992$j9$l0$h0",
            "_ga_C98RVP2QRJ": "GS2.1.s1749221684$o1$g1$t1749221992$j9$l0$h0",
        }
        cookies.update(self.shield_cookies)
        return cookies

    def _get_product_headers(self):
        return {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "referer": "https://validate.perfdrive.com/",
            "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": self._get_current_user_agent(),
        }

    def _get_shipping_headers(self):
        return {
            "accept": "*/*",
            "accept-language": "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiTWl4ZXIgRGVzayIsImNoYW5uZWwiOnsibmFtZSI6Im1peGVyLWRlc2subWFnYXppbmVsdWl6YS5jb20uYnIifSwiaWF0IjoxNzUwMjc5MjYwLCJleHAiOjE3NTA0OTUyNjB9.Ss8mtRcggxLIFu5DIBdPX5MmK1rKstqjKZJznymrimY",
            "content-type": "application/json",
            "origin": "https://www.magazineluiza.com.br",
            "priority": "u=1, i",
            "referer": "https://www.magazineluiza.com.br/",
            "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": self._get_current_user_agent(),
        }

    def _extract_path_parts(self, path_url):
        path = path_url.rstrip("/")
        path_json = f"{path}.json"
        path_0 = path.split("/p")[0].rstrip("/")
        path_2 = path.split("/p/")[1].split("/")[0]
        path_3 = path.split("/")[-2]
        path_4 = path.split("/")[-1]

        return {"path_json": path_json, "path_0": path_0, "path_2": path_2, "path_3": path_3, "path_4": path_4}

    def _prepare_shipping_payload(self, product_data, sku, seller_id, installment_price):
        price = product_data["props"]["pageProps"]["data"]["product"]["price"]
        product = product_data["props"]["pageProps"]["data"]["product"]
        structure = product_data["props"]["pageProps"]["structure"]

        return {
            "operationName": "shippingQuery",
            "variables": {
                "shippingRequest": {
                    "metadata": {
                        "categoryId": structure["route"]["categoryId"],
                        "clientId": "",
                        "organizationId": "magazine_luiza",
                        "pageName": "",
                        "partnerId": "0",
                        "salesChannelId": structure["config"]["salesChannelId"],
                        "sellerId": seller_id,
                        "subcategoryId": structure["route"]["subCategoryId"],
                    },
                    "product": {
                        "currency": price.get("currency"),
                        "dimensions": product.get("dimensions", {}),
                        "exchangeRate": price.get("exchangeRate"),
                        "id": sku,
                        "idExchangeRate": price.get("idExchangeRate"),
                        "originalPriceForeign": price.get("originalPriceForeign"),
                        "price": installment_price,
                        "quantity": 1,
                        "type": product["type"],
                    },
                    "zipcode": self.cep,
                }
            },
            "query": """
                query shippingQuery($shippingRequest: ShippingRequest!) {
                    shipping(shippingRequest: $shippingRequest) {
                        status
                        ...shippings
                        ...estimate
                        ...estimateError
                        __typename
                    }
                }

                fragment estimateError on EstimateErrorResponse {
                    error
                    status
                    message
                    uuid
                    __typename
                }

                fragment estimate on EstimateResponse {
                    disclaimers { sequence message __typename }
                    deliveries {
                        id
                        items {
                            bundleComposition { quantity sku __typename }
                            gifts { quantity sku __typename }
                            quantity
                            seller { id name sku __typename }
                            type
                            __typename
                        }
                        modalities {
                            id
                            type
                            name
                            serviceProviders
                            shippingTime {
                                unit
                                value { min max __typename }
                                description
                                disclaimers { sequence message __typename }
                                __typename
                            }
                            campaigns { id name skus __typename }
                            cost { customer operation __typename }
                            prices { customer operation currency exchangeRate __typename }
                            zipCodeRestriction
                            __typename
                        }
                        provider { id __typename }
                        status { code message __typename }
                        __typename
                    }
                    shippingAddress { city district ibge latitude longitude prefixZipCode state street zipCode __typename }
                    status
                    __typename
                }

                fragment shippings on ShippingResponse {
                    status
                    shippings {
                        id
                        name
                        packages {
                            price
                            seller
                            sellerDescription
                            deliveryTypes { id description type time price __typename }
                            __typename
                        }
                        __typename
                    }
                    __typename
                }
                """,
        }

    def _create_availability_data(self, product_info, shipping_info, url):
        now = datetime.now()
        shipping = shipping_info.get("data", {}).get("shipping", {})
        deliveries = shipping.get("deliveries", [])

        # Modificação aqui: verifica o message quando não há disponibilidade
        if not deliveries and shipping.get("message"):
            shipping_cost = shipping.get("message")
            modalities = []
            shipping_time = {}
            city = self.city  # Usa a cidade do fallback
        else:
            modalities = deliveries[0].get("modalities", []) if deliveries else []
            shipping_cost = modalities[0].get("cost", {}).get("customer") if modalities else None
            shipping_time = modalities[0].get("shippingTime", {}).get("value", {}) if modalities else {}
            city = shipping.get("shippingAddress", {}).get("city", self.city)  # Usa o fallback se não houver cidade no shipping

        return {
            # "type": "preco",
            "ean": self.search_term,
            "sku_casas_bahia": self.sku_casas_bahia,
            "sku_concorrente": product_info['sku'],
            "codigo_site": product_info['codigo_site'],
            "site_concorrente": None,
            "vendido_por": product_info["vendido_por"],
            "preco_a_prazo": product_info["preco_a_prazo"],
            "preco_a_vista": product_info["preco_a_vista"],
            "preco_exclusivo": None,
            "data": now.strftime("%Y-%m-%d"),
            "hora": now.strftime("%H:%M:%S"),
            "estoque": "S",
            "qdt_parcelas_preco_sugerido_site": product_info["qdt_parcelas_preco_sugerido_site"],
            "valor_parcelas_preco_sugerido_site": product_info["valor_parcelas_preco_sugerido_site"],
            "taxa_juros_preco_sugerido_site": product_info["taxa_juros_preco_sugerido_site"],
            "qtde_max_parcelas_cartao_bandeirado": product_info["qtde_max_parcelas_cartao_bandeirado"],
            "valor_parcelas_cartao_bandeirado": product_info["valor_parcelas_cartao_bandeirado"],
            "taxa_juros_cartao_bandeirado": product_info["taxa_juros_cartao_bandeirado"],
            "qtde_max_parcelas_cartao_proprio": product_info["qtde_max_parcelas_cartao_proprio"],
            "valor_parcelas_cartao_proprio": product_info["valor_parcelas_cartao_proprio"],
            "taxa_juros_cartao_proprio": product_info["taxa_juros_cartao_proprio"],
            "cep": self.cep,
            "cidade": city,
            "tipo_frete": modalities[0].get("name") if modalities else None,
            "valor_frete": shipping_cost,
            "prazo_entrega": shipping_time.get("max"),
            "url": url,
        }

    # def _create_unavailability_data(self):
    #     now = datetime.now()

    #     return {
    #         "type": "indisponibilidade",
    #         "ean": self.search_term,
    #         "sku_casas_bahia": self.sku_casas_bahia,
    #         "error_message": "Não há ofertas disponíveis para o EAN informado",
    #         "data": now.strftime("%Y-%m-%d"),
    #         "hora": now.strftime("%H:%M:%S"),
    #         "estoque": "N",
    #         "cep": self.cep,
    #         "cidade": self.city,
    #     }

    def save_result(self, result):
        with open("./data/magalu_feed.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")

    def process_product(self, product):
        try:
            # Extract basic product info
            seller = product.get("seller", {})
            installment = product.get("installment", {})
            price = product.get("price", {})
            path = product.get("path")

            # Get product details
            paths = self._extract_path_parts(path)
            product_url = f"https://www.magazineluiza.com.br/{paths['path_0']}/p/{paths['path_2']}/{paths['path_3']}/{paths['path_4']}/"

            product_data = self.api.get_product_details(product_url)
            if not product_data:
                return None

            # Prepare data for shipping request
            seller_id = product_data["props"]["pageProps"]["data"]["product"]["seller"].get("id")
            site_code = product_data["props"]["pageProps"]["data"]["product"].get("id")

            payment_methods = product_data["props"]["pageProps"]["data"]["product"].get("paymentMethods")

            # Initialize payment method variables
            qtde_max_parcelas_cartao_proprio = None
            valor_parcela_cartao_proprio = None
            taxa_juros_cartao_proprio = None
            qtde_max_parcelas_cartao_bandeirado = None
            valor_parcela_cartao_bandeirado = None
            taxa_juros_cartao_bandeirado = None

            # Extract payment method data
            if payment_methods:
                for method in payment_methods:
                    method_id = method.get("id")

                    if method_id == "luiza_ouro":
                        plans = method.get("installmentPlans", [])
                        if plans:
                            last_plan = plans[-1]  # Último plano de parcelamento
                            qtde_max_parcelas_cartao_proprio = last_plan.get("installment")
                            valor_parcela_cartao_proprio = last_plan.get("installmentAmount")
                            taxa_juros_cartao_proprio = last_plan.get("interest")

                    elif method_id not in ["account_balance", "boleto", "pix"]:
                        # Pega dados de outras bandeiras (mastercard, visa, etc)
                        plans = method.get("installmentPlans", [])
                        if plans:
                            last_plan = plans[-1]  # Último plano de parcelamento
                            qtde_max_parcelas_cartao_bandeirado = last_plan.get("installment")
                            valor_parcela_cartao_bandeirado = last_plan.get("installmentAmount")
                            taxa_juros_cartao_bandeirado = last_plan.get("interest")

            shipping_payload = self._prepare_shipping_payload(product_data, seller.get("sku"), seller_id, float(price.get("fullPrice")))

            shipping_data = self.api.get_shipping(shipping_payload)
            if not shipping_data:
                return None

            # Create product info dictionary
            product_info = {
                "sku": seller.get("sku"),
                "codigo_site": site_code,
                "vendido_por": seller.get("description"),
                "preco_a_prazo": float(price.get("fullPrice")),
                "preco_a_vista": float(price.get("bestPrice")),
                "qdt_parcelas_preco_sugerido_site": installment.get("quantity"),
                "valor_parcelas_preco_sugerido_site": float(installment.get("amount")),
                "taxa_juros_preco_sugerido_site": installment.get("paymentMethodDescription"),
                "qtde_max_parcelas_cartao_bandeirado": qtde_max_parcelas_cartao_bandeirado,
                "valor_parcelas_cartao_bandeirado": valor_parcela_cartao_bandeirado,
                "taxa_juros_cartao_bandeirado": taxa_juros_cartao_bandeirado,
                "qtde_max_parcelas_cartao_proprio": qtde_max_parcelas_cartao_proprio,
                "valor_parcelas_cartao_proprio": valor_parcela_cartao_proprio,
                "taxa_juros_cartao_proprio": taxa_juros_cartao_proprio,
            }

            return self._create_availability_data(product_info, shipping_data, product_url)

        except Exception as e:
            logging.error(f"Error processing product: {e}")
            return None

    def process_products(self):
        products = self.api.search_products(self.search_url)

        if products and products.get("products", []):
            for product in products["products"]:
                availability = self.process_product(product)
                if availability:
                    self.results.append(availability)
                    self.save_result(availability)
                    self.processed_count += 1
        else:
            # unavailability = self._create_unavailability_data()
            # self.results.append(unavailability)
            # self.save_result(unavailability)
            # self.processed_count += 1
            # logging.warning(f"No products found for EAN: {self.search_term}")
            # Criar dados de indisponibilidade usando _create_availability_data
            now = datetime.now()
            unavailability = {
                # "type": "indisponibilidade",
                "ean": self.search_term,
                "sku_casas_bahia": self.sku_casas_bahia,
                "sku_concorrente": None,
                "codigo_site": None,
                "site_concorrente": None,
                "vendido_por": None,
                "preco_a_prazo": None,
                "preco_a_vista": None,
                "preco_exclusivo": None,
                "data": now.strftime("%Y-%m-%d"),
                "hora": now.strftime("%H:%M:%S"),
                "estoque": "N",
                "qdt_parcelas_preco_sugerido_site": None,
                "valor_parcelas_preco_sugerido_site": None,
                "taxa_juros_preco_sugerido_site": None,
                "qtde_max_parcelas_cartao_bandeirado": None,
                "valor_parcelas_cartao_bandeirado": None,
                "taxa_juros_cartao_bandeirado": None,
                "qtde_max_parcelas_cartao_proprio": None,
                "valor_parcelas_cartao_proprio": None,
                "taxa_juros_cartao_proprio": None,
                "cep": self.cep,
                "cidade": self.city,
                "tipo_frete": None,
                "valor_frete": None,
                "prazo_entrega": None,
                "url": None,
            }

            self.results.append(unavailability)
            self.save_result(unavailability)
            self.processed_count += 1
            # logging.warning(f"No products found for EAN: {self.search_term}")

        # logging.info(f"{self.processed_count} records processed")
