import xml.etree.ElementTree as ET
from datetime import datetime

import psycopg2
from psycopg2 import sql


def create_connection():
    return psycopg2.connect(
        dbname="db",
        user="user",
        password="password",
        host="localhost",
        port="5432",
    )


def insert_sku(connection, sku_data):
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
        """
        )

        cursor.execute(insert_query, sku_data)


def process_xml_file(xml_file):
    connection = create_connection()
    with connection:
        for event, elem in ET.iterparse(xml_file, events=("start", "end")):
            if event == "end" and elem.tag == "offer":
                sku_data = {
                    "uuid": elem.get("id"),
                    "marketplace_id": int(elem.find("marketplaceId").text),
                    "product_id": int(elem.find("productId").text),
                    "title": elem.find("name").text,
                    "description": elem.find("description").text,
                    "brand": int(elem.find("brand").text),
                    "seller_id": int(elem.find("sellerId").text),
                    "seller_name": elem.find("sellerName").text,
                    "first_image_url": elem.find("firstImageUrl").text,
                    "category_id": int(elem.find("categoryId").text),
                    "category_lvl_1": elem.find("categoryLvl1").text,
                    "category_lvl_2": elem.find("categoryLvl2").text,
                    "category_lvl_3": elem.find("categoryLvl3").text,
                    "category_remaining": elem.find("categoryRemaining").text,
                    "features": elem.find("features").text,
                    "rating_count": int(elem.find("ratingCount").text),
                    "rating_value": float(elem.find("ratingValue").text),
                    "price_before_discounts": float(
                        elem.find("priceBeforeDiscounts").text
                    ),
                    "discount": float(elem.find("discount").text),
                    "price_after_discounts": float(
                        elem.find("priceAfterDiscounts").text
                    ),
                    "bonuses": int(elem.find("bonuses").text),
                    "sales": int(elem.find("sales").text),
                    "inserted_at": datetime.now(),
                    "updated_at": datetime.now(),
                    "currency": elem.find("currency").text,
                    "barcode": int(elem.find("barcode").text),
                }
                insert_sku(connection, sku_data)
                elem.clear()


if __name__ == "__main__":
    xml_file_path = "elektronika_products_20240423_114945.xml"
    process_xml_file(xml_file_path)
