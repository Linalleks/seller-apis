import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список всех товаров продавца на озон.

    Args:
        last_id (str): Идентификатор последнего товара на предыдущей странице.
            При первом запросе нужно передать пустое значение.
            Чтобы получить следующие значения, указать last_id из ответа предыдущего запроса.
        client_id (str): Идентификатор клиента-продавца озон.
        seller_token (str): Токен от АПИ продавца на озон.

    Returns:
        dict: Список товаров - "items" (не более 1000),
            идентификатор последнего товара на странице - "last_id",
            количество всех товаров продавца - "total".

    Examples:
        correct execution:
            >>> print(get_product_list("", "123456", "82a02da882a02da882a02da8a981b7f3cc882a082a02da8e4af9c41e8551329276dde72"))
            {
                "items": [
                    {
                        "archived": true,
                        "has_fbo_stocks": true,
                        "has_fbs_stocks": true,
                        "is_discounted": true,
                        "offer_id": "136748",
                        "product_id": 223681945,
                        "quants": [
                            {
                                "quant_code": "string",
                                "quant_size": 0
                            }
                        ]
                    }
                ],
                "total": 1,
                "last_id": "bnVсbA=="
            }

        incorrect execution:
            >>> print(get_product_list("", "123456", ""))
            requests.exceptions.HTTPError: 401 Client Error
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получает артикулы всех товаров продавца на озон.

    Args:
        client_id (str): Идентификатор клиента-продавца озон.
        seller_token (str): Токен от АПИ продавца на озон.

    Returns:
        list: Список артикулов всех товаров продавца.

    Examples:
        correct execution:
            >>> print(get_offer_ids("123456", "82a02da882a02da882a02da8a981b7f3cc882a082a02da8e4af9c41e8551329276dde72"))
            ['136748', '136749']

        incorrect execution:
            >>> print(get_offer_ids("123456", ""))
            requests.exceptions.HTTPError: 401 Client Error
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновляет цены товаров продавца на озон.

    Args:
        prices (list): Список цен для обновления
        client_id (str): Идентификатор клиента-продавца озон.
        seller_token (str): Токен от АПИ продавца на озон.

    Returns:
        dict: Отчет об обновлении цен по каждому товару.

    Examples:
        correct execution:
            >>> print(update_price([],"123456", "82a02da882a02da882a02da8a981b7f3cc882a082a02da8e4af9c41e8551329276dde72"))
            {
                "result": [
                    {
                        "product_id": 1386,
                        "offer_id": "PH8865",
                        "updated": true,
                        "errors": []
                    }
                ]
            }

        incorrect execution:
            >>> print(get_offer_ids([], "123456", ""))
            requests.exceptions.HTTPError: 401 Client Error
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновляет остатки товаров продавца на озон.

    Args:
        stocks (list): Список товаров с их количеством для обновления.
        client_id (str): Идентификатор клиента-продавца озон.
        seller_token (str): Токен от АПИ продавца на озон.

    Returns:
        dict: Отчет об обновлении остатков по каждому товару.

    correct execution:
            >>> print(update_price([],"123456", "82a02da882a02da882a02da8a981b7f3cc882a082a02da8e4af9c41e8551329276dde72"))
            {
                "result": [
                    {
                        "warehouse_id": 22142605386000,
                        "product_id": 118597312,
                        "offer_id": "PH11042",
                        "updated": true,
                        "errors": [ ]
                    }
                ]
            }

        incorrect execution:
            >>> print(update_price([], "123456", ""))
            requests.exceptions.HTTPError: 401 Client Error
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Получает остатки товаров с сайта timeworld.ru.

    Returns:
        list of dict: Список товаров со следующими полями: Код, Наименование товара, Изображение, Цена, Количество, Заказ.

    Examples:
        correct execution:
            >>> print(download_stock())
            [
                {
                    'Код': 69791,
                    'Наименование товара': 'Украшение для дисплеев 219RU-GSGSTDUMMY2',
                    'Изображение': 'Показать',
                    'Цена': '550.00 руб.',
                    'Количество': '>10',
                    'Заказ': ''
                },
                ...и т.д.
            ]
        incorrect execution:
            >>> print(download_stock())
            requests.exceptions.HTTPError: 403 Client Error
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Формирует список всех товаров продавца на озон с новыми значениями количества, а именно:
    если товар с сайта timeworld.ru есть у продавца на озон, то количество записывается по условиям:
        если на сайте = ">10", то = 100,
        если на сайте = "1", то = 0,
        иначе, то = количество с сайта.
    Для товаров продавца на озон, которых нет на сайте timeworld.ru, записывается количество = 0.

    Args:
        watch_remnants (list of dict): Список остатков товаров с сайта timeworld.ru.
        offer_ids (list): Список артикулов товаров продавца на озон.

    Returns:
        list of dict: Список товаров с данными о количестве.

    Examples:
        correct execution:
            >>> print(create_stocks([{'Код': 69791, 'Количество': '>10'}], ['69791', '70000']))
            [{'offer_id': '69791', 'stock': 100}, {'offer_id': '70000', 'stock': 0}]

        incorrect execution:
            >>> print(create_stocks({'Код': 69791, 'Количество': '>10'}, ['69791', '70000']))
            AttributeError: 'str' object has no attribute 'get'
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Формирует список товаров продавца на озон, которые есть на сайте timeworld.ru, с новыми значениями цен.

    Args:
        watch_remnants (list of dict): Список остатков товаров с сайта timeworld.ru.
        offer_ids (list): Список артикулов товаров продавца на озон.

    Returns:
        list of dict: Список товаров с данными о цене.

    correct execution:
            >>> print(create_prices([{'Код': 69791, 'Цена': '550.00 руб.'}], ['69791', '70000']))
            [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '69791', 'old_price': '0', 'price': '550'}]

        incorrect execution:
            >>> print(create_prices({'Код': 69791, 'Количество': '>10'}, ['69791', '70000']))
            AttributeError: 'str' object has no attribute 'get'
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Оставляет в строке с ценой только цифры без дробной части после точки.

    Args:
        price (str): Строка с ценой.

    Returns:
        str: Строка из целого числа.

    Examples:
        correct execution:
            >>> print(price_conversion("5'990.00 руб."))
            5990

        incorrect execution:
            >>> print(price_conversion("5'990,00 руб."))
            599000

            >>> print(price_conversion(5990.00))
            AttributeError: 'float' object has no attribute 'split'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Делит список lst на части по n элементов

    Args:
        lst (list): Любой список.
        n (int): Макс. количество элементов списка в одной части.

    Yields:
        list: Следующая часть списка из n (или < n) элементов.

    Examples:
        correct execution:
            >>> print([i for i in divide([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 5)])
            [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12]]

        incorrect execution:
            >>> print([i for i in divide({'Код': 69791, 'Количество': '>10', 'Цена': 250}, 2)])
            KeyError: slice(0, 2, None)
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Обновляет цены товаров продавца на озон.

    Args:
        watch_remnants (list of dict): Список остатков товаров с сайта timeworld.ru.
        client_id (str): Идентификатор клиента-продавца озон.
        seller_token (str): Токен от АПИ продавца на озон.

    Returns:
        list of dict: Список товаров с данными о цене.

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Обновляет остатки товаров продавца на озон.

    Args:
        watch_remnants (list of dict): Список остатков товаров с сайта timeworld.ru.
        client_id (str): Идентификатор клиента-продавца озон.
        seller_token (str): Токен от АПИ продавца на озон.

    Returns:
        not_empty (list of dict): Список товаров, количество которых больше 0, с данными о количестве.
        stocks (list of dict): Список товаров с данными о количестве.

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
