import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список всех товаров магазина на Яндекс Маркете.

    Args:
        page (str): Идентификатор страницы c результатами.
            При первом запросе нужно передать пустую строку.
            Чтобы получить результаты следующих страниц, указать nextPageToken из ответа предыдущего запроса.
        campaign_id (int): Идентификатор магазина на Яндекс маркете для работы с API.
        access_token (str): API-Key-токен с доступами к определенным группам API-методов Яндекс Маркета.

    Returns:
        dict: Список товаров - "items" (не более 200),
            идентификатор последнего товара на странице - "last_id",
            количество всех товаров продавца - "total".

    Examples:
        correct execution:
            >>> print(get_product_list("", 123456, "y0_BfRRRRRV2L8sWWvNkSNNNNSrLHaNXg4cCMswFbL6MWab9lktL2KPsMw"))
            {
                "paging": {"nextPageToken": "eyBuZXh0SWQ6IDIzNDIgfQ==", "prevPageToken": ""},
                "offerMappingEntries": [
                    {
                        "offer": {
                            "name": "Украшение для дисплеев 219RU-GSGSTDUMMY2",
                            "shopSku": "69791",
                            "category": "Дополнения к часам",
                            "vendor": "Casio",
                            "vendorCode": "VNDR-0005A",
                            "description": "",
                            "id": "1234",
                            ...и т.д.
                        },
                        ...и т.д.
                    }
                ]
            }

        incorrect execution:
            >>> print(get_product_list("", 123456, ""))
            requests.exceptions.HTTPError: 401 Client Error
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет остатки товаров магазина на Яндекс Маркете.

    Args:
        stocks (list of dict): Список товаров с их количеством для обновления.
        campaign_id (int): Идентификатор магазина на Яндекс маркете для работы с API.
        access_token (str): API-Key-токен с доступами к определенным группам API-методов Яндекс Маркета.

    Returns:
        dict: Отчет об обновлении остатков.

    Examples:
        correct execution:
            >>> print(update_stocks([{'id': '69791', 'price': {'value': 550, 'currencyId': 'RUR'}}], 123456, "y0_BfRRRRRV2L8sWWvNkSNNNNSrLHaNXg4cCMswFbL6MWab9lktL2KPsMw"))
            {"status": "OK"}

        incorrect execution:
            >>> print(update_stocks([], "123456", ""))
            requests.exceptions.HTTPError: 401 Client Error
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет цены товаров магазина на Яндекс Маркете.

    Args:
        prices (list of dict): Список товаров с их ценами для обновления.
        campaign_id (int): Идентификатор магазина на Яндекс маркете для работы с API.
        access_token (str): API-Key-токен с доступами к определенным группам API-методов Яндекс Маркета.

    Returns:
        dict: Отчет об обновлении цен.

    Examples:
        correct execution:
            >>> print(update_price([{'id': '69791', 'price': {'value': 550, 'currencyId': 'RUR'}}], 123456, "y0_BfRRRRRV2L8sWWvNkSNNNNSrLHaNXg4cCMswFbL6MWab9lktL2KPsMw"))
            {"status": "OK"}

        incorrect execution:
            >>> print(update_price([], "123456", ""))
            requests.exceptions.HTTPError: 401 Client Error
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получает артикулы всех товаров магазина на Яндекс Маркете.

    Args:
        campaign_id (int): Идентификатор магазина на Яндекс маркете для работы с API.
        market_token (str): API-Key-токен с доступами к определенным группам API-методов Яндекс Маркета.

    Returns:
        list: Список артикулов всех товаров магазина на Яндекс Маркете.
    
    Examples:
        correct execution:
            >>> print(get_offer_ids("123456", "y0_BfRRRRRV2L8sWWvNkSNNNNSrLHaNXg4cCMswFbL6MWab9lktL2KPsMw"))
            ['136748', '136749']

        incorrect execution:
            >>> print(get_offer_ids("123456", ""))
            requests.exceptions.HTTPError: 401 Client Error
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Формирует список всех товаров магазина на Яндекс Маркете с новыми значениями количества, а именно:
    если товар с сайта timeworld.ru есть в магазине на Яндекс Маркете, то количество записывается по условиям:
        если на сайте = ">10", то = 100,
        если на сайте = "1", то = 0,
        иначе, то = количество с сайта.
    Для товаров магазина на Яндекс Маркете, которых нет на сайте timeworld.ru, записывается количество = 0.

    Args:
        watch_remnants (list of dict): Список остатков товаров с сайта timeworld.ru.
        offer_ids (list): Список артикулов товаров магазина на Яндекс Маркете.
        warehouse_id (int): Идентификатор склада на Яндекс маркете.

    Returns:
        list of dict: Список товаров с данными о количестве.
    
    Examples:
        correct execution:
            >>> print(create_stocks([{'Код': 69791, 'Количество': '>10'}], ['69791', '70000']), 1)
            [
                {'sku': '69791', 'warehouseId': 1, 'items': [{'count': 100, 'type': 'FIT', 'updatedAt': '2025-12-14T16:30:31Z'}]}, 
                {'sku': '70000', 'warehouseId': 1, 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2025-12-14T16:30:31Z'}]}
            ]

        incorrect execution:
            >>> print(create_stocks([{'Код': 69791, 'Количество': '>10'}], ['69791', '70000']))
            TypeError: create_stocks1() missing 1 required positional argument: 'warehouse_id'
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Формирует список товаров магазина на Яндекс Маркете, которые есть на сайте timeworld.ru, с новыми значениями цен.

    Args:
        watch_remnants (list of dict): Список остатков товаров с сайта timeworld.ru.
        offer_ids (list): Список артикулов товаров магазина на Яндекс Маркете.

    Returns:
        list of dict: Список товаров с данными о цене.
    
    Examples:
        correct execution:
            >>> print(create_prices([{'Код': 69791, 'Цена': '550.00 руб.'}], ['69791', '70000']))
            [{'id': '69791', 'price': {'value': 550, 'currencyId': 'RUR'}}]

        incorrect execution:
            >>> print(create_prices({'Код': 69791, 'Количество': '>10'}, ['69791', '70000']))
            AttributeError: 'str' object has no attribute 'get'
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Получает список товаров магазина на Яндекс Маркете, которые есть на сайте timeworld.ru, с новыми значениями цен
    и обновляет цены товаров магазина на Яндекс Маркете.

    Args:
        watch_remnants (list of dict): Список остатков товаров с сайта timeworld.ru.
        campaign_id (int): Идентификатор магазина на Яндекс маркете для работы с API.
        market_token (str): API-Key-токен с доступами к определенным группам API-методов Яндекс Маркета.

    Returns:
        list of dict: Список товаров с данными о цене.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Получает список всех товаров магазина на Яндекс Маркете с новыми значениями количества
    и обновляет остатки товаров магазина на Яндекс Маркете.

    Args:
        watch_remnants (list of dict): Список остатков товаров с сайта timeworld.ru.
        campaign_id (int): Идентификатор магазина на Яндекс маркете для работы с API.
        market_token (str): API-Key-токен с доступами к определенным группам API-методов Яндекс Маркета.
        warehouse_id (int): Идентификатор склада на Яндекс маркете.

    Returns:
        not_empty (list of dict): Список товаров, количество которых больше 0, с данными о количестве.
        stocks (list of dict): Список товаров с данными о количестве.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
