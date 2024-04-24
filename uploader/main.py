import json
import logging
import os
import uuid
from datetime import datetime
from typing import Iterable, Tuple

import psycopg2
from dotenv import load_dotenv
from lxml import etree
from psycopg2 import sql

load_dotenv()
logging.basicConfig(filename="data_uploader.log", level=logging.INFO)


def create_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )


def _g_process_et_items(path, tag) -> Iterable[Tuple]:
    logging.info("_g_process_et_items")

    context: etree.ElementTree = etree.iterparse(
        path, events=("end",), tag=tag
    )
    for event, elem in context:
        yield event, elem

        logging.info("Generator element: %s", elem)
        elem.clear()


def convert_timestamp(timestamp):
    return datetime.utcfromtimestamp(int(timestamp))


def insert_sku(sku_data):
    logging.info("Insert_sku")
    connection = create_connection()
    logging.info("Success create connection")
    with connection.cursor() as cursor:
        insert_query = sql.SQL(
            """
            INSERT INTO public.sku (
                uuid, marketplace_id, product_id, title, description,
                brand, seller_id, seller_name, first_image_url,
                category_id, category_lvl_1, category_lvl_2, category_lvl_3,
                category_remaining, features, rating_count, rating_value,
                price_before_discounts, discount, price_after_discounts,
                bonuses, sales, inserted_at, updated_at, currency, barcode
            ) VALUES (
                %(uuid)s, %(marketplace_id)s, %(product_id)s, %(title)s,
                %(description)s, %(brand)s, %(seller_id)s, %(seller_name)s,
                %(first_image_url)s, %(category_id)s, %(category_lvl_1)s,
                %(category_lvl_2)s, %(category_lvl_3)s, %(category_remaining)s,
                %(features)s, %(rating_count)s, %(rating_value)s,
                %(price_before_discounts)s, %(discount)s,
                %(price_after_discounts)s, %(bonuses)s, %(sales)s,
                %(inserted_at)s, %(updated_at)s, %(currency)s, %(barcode)s
            )
            RETURNING *
            """
        )
        try:
            cursor.execute(insert_query, sku_data)
            inserted_row = cursor.fetchone()
            connection.commit()

            logging.info(
                "Inserted SKU with UUID: %s. Inserted row: %s",
                sku_data["uuid"],
                inserted_row,
            )

        except Exception as e:
            connection.rollback()
            logging.error(
                "Failed to insert SKU with UUID: %s. Error: %s",
                sku_data["uuid"],
                e,
            )


def get_element_text(element, tag):
    found_element = element.find(tag)
    return found_element.text if found_element is not None else None


def process_xml_file(xml_file_path):
    for event, elem in _g_process_et_items(xml_file_path, "offer"):
        logging.info("Started process with elem: %s", elem)
        params = {}
        for param_elem in elem.findall("param"):
            param_name = param_elem.get("name")
            param_value = param_elem.text
            params[param_name] = param_value

        features_json = json.dumps(params)

        sku_data = {
            "uuid": str(uuid.uuid4()),
            "marketplace_id": elem.get("marketplace_id"),
            "product_id": elem.get("id"),
            "title": get_element_text(elem, "name"),
            "description": get_element_text(elem, "description"),
            "brand": get_element_text(elem, "vendor"),
            "seller_id": elem.get("seller_id"),
            "seller_name": "",
            "first_image_url": get_element_text(elem, "picture"),
            "category_id": get_element_text(elem, "categoryId"),
            "category_lvl_1": "",
            "category_lvl_2": "",
            "category_lvl_3": "",
            "category_remaining": "",
            "features": features_json,
            "rating_count": (
                elem.get("rating_count")
                if elem.get("rating_count") is not None
                else None
            ),
            "rating_value": (
                elem.get("rating_value")
                if elem.get("rating_value") is not None
                else None
            ),
            "price_before_discounts": get_element_text(elem, "oldprice"),
            "discount": (
                elem.get("discount")
                if elem.get("discount") is not None
                else None
            ),
            "price_after_discounts": get_element_text(elem, "price"),
            "bonuses": (
                elem.get("bonuses")
                if elem.get("bonuses") is not None
                else None
            ),
            "sales": (
                elem.get("sales") if elem.get("sales") is not None else None
            ),
            "inserted_at": (
                convert_timestamp(get_element_text(elem, "inserted_at"))
                if elem.get("inserted_at") is not None
                else None
            ),
            "updated_at": (
                convert_timestamp(get_element_text(elem, "modified_time"))
                if elem.get("modified_time") is not None
                else None
            ),
            "currency": get_element_text(elem, "currencyId"),
            "barcode": (
                get_element_text(elem, "barcode")
                if elem.find("barcode") is not None
                else None
            ),
        }
        insert_sku(sku_data)


if __name__ == "__main__":
    xml_file_path = "uploader/elektronika_products_20240423_114945.xml"
    process_xml_file(xml_file_path)
