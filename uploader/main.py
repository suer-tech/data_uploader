import psycopg2


def create_connection():
    return psycopg2.connect(
        dbname="db",
        user="user",
        password="password",
        host="localhost",
        port="5432",
    )


if __name__ == "__main__":
    xml_file_path = "elektronika_products_20240423_114945.xml"
