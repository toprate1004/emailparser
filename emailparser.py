import os
import base64
import pymysql
import re
import csv
import json

from bs4 import BeautifulSoup
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from email import message_from_bytes
from datetime import datetime, timedelta
from pymysql import Error

from dotenv import load_dotenv

load_dotenv()

# Function create_connection() - Connect to database
def create_connection(host_name, user_name, user_password, db_name):
    """Create a connection to the MySQL database."""
    connection = None
    try:
        connection = pymysql.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
        with connection.cursor() as cursor:
            # SQL query to create the table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS container (
                id INT AUTO_INCREMENT PRIMARY KEY,
                location VARCHAR(255),
                quantity INT(5),
                size VARCHAR(255),
                term VARCHAR(255),
                price int(7),
                feature VARCHAR(255),
                depot VARCHAR(255),
                ETA VARCHAR(255),
                provider VARCHAR(255),
                vendor VARCHAR(255),
                received_date VARCHAR(255),
                created_date VARCHAR(255)
            );
            """
            cursor.execute(create_table_query)
            print("Table 'container' created successfully!")

        # Commit changes
        connection.commit()
    except Exception as e:
        print(f"The error {str(e)} occurred")

    return connection

# Function execute_query() - Execute query
def execute_query(connection, query):
    """Execute a single query."""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Exception as e:
        print(f"The error '{e}' occurred")

def insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email, received_date, created_date):

    insert = f"""
    INSERT INTO container (size, quantity, term, location, price, feature, depot, ETA, provider, vendor, received_date, created_date)
    VALUES ('{size}', '{quantity}', '{term}', '{location}', '{price}', '{feature}', '{depot}', '{eta}', '{provider}', '{vendor_email}', '{received_date}', '{created_date}')
    """
    execute_query(connection, insert)

def get_container_data():
    # Connect to the MySQL database
    host = "localhost"
    user = "root"
    password = os.getenv("MYSQL_PASSWORD")
    database = "container"

    # Create a connection
    connection = create_connection(host, user, password, database)

    try:
        with connection.cursor() as cursor:
            # SQL query to fetch data
            fetch_query = "SELECT * FROM container ORDER BY created_date DESC"
            cursor.execute(fetch_query)

            # Fetch all results
            container_data = cursor.fetchall()
            container_json_data = [{"location": row[1], "quantity": row[2],
                                    "size": row[3], "term": row[4],
                                    "price": row[5], "feature": row[6],
                                    "depot": row[7], "eta": row[8],
                                    "provider": row[9], "vendor": row[10],
                                    "received_date": row[11], "created_date": row[12]
                                    } for row in container_data]

    except Exception as e:
        print("Error fetching data:", e)

    # Close the connection
    if connection:
        connection.close()

    return container_json_data

def clear_container_data(email):
    # Connect to the MySQL database
    host = "localhost"
    user = "root"
    password = os.getenv("MYSQL_PASSWORD")
    database = "container"

    # Create a connection
    connection = create_connection(host, user, password, database)

    try:
        with connection.cursor() as cursor:
            # SQL query to fetch data
            query = f"SELECT COUNT(*) FROM container WHERE vendor = '{email}'"
            cursor.execute(query)
            count = cursor.fetchone()[0]

            if count > 0:
                query = "DELETE FROM container WHERE vendor = %s"
                cursor.execute(query, (email,))

                # Commit changes
                connection.commit()
                print("Rows deleted successfully.")

    except Exception as e:
        print("Error deleting data:", e)

    # Close the connection
    if connection:
        connection.close()

    return

def export_to_csv(filename):
    # Connect to the MySQL database
    host = "localhost"
    user = "root"
    password = os.getenv("MYSQL_PASSWORD")
    database = "container"

    # Create a connection
    connection = create_connection(host, user, password, database)

    try:
        with connection.cursor() as cursor:
            # SQL query to fetch data
            fetch_query = "SELECT * FROM container"
            cursor.execute(fetch_query)

            # Fetch all data
            rows = cursor.fetchall()

            # Get column names
            column_names = [i[0] for i in cursor.description]

            modified_rows = []
            for row in rows:
                row = list(row)  # Convert tuple to list for mutability
                modified_rows.append(row)

            # Write to CSV
            with open(filename, mode='w', newline='') as file:
                writer = csv.writer(file)

                # Write column headers
                writer.writerow(column_names)

                # Write data rows
                writer.writerows(modified_rows)

            print(f"Data successfully exported to {filename}")

    except Exception as e:
        print("Error:", e)

    # Close the connection
    if connection:
        connection.close()

# If modifying these SCOPES, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Function authenticate_gmail() - Access gmail
def authenticate_gmail():
    """Authenticates and returns a Gmail API service object."""
    creds = None
    # The token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no valid credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=3000)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    # Build the Gmail API service
    service = build('gmail', 'v1', credentials=creds)
    return service

# Function get_messages() - Read gmail
def get_messages(service, query=''):

    """Get messages that match the query from the user's Gmail."""
    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])

    if not messages:
        print("No messages found.")
    else:
        print(f"Found {len(messages)} messages.")

    return messages

# Function get_message_content() - Parse gmail
def get_message_content_html(service, message_id):
    """Retrieve a specific message by its ID and decode its content."""
    message = service.users().messages().get(userId='me', id=message_id, format='raw').execute()
    msg_raw = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
    email_message = message_from_bytes(msg_raw)

    # Connect to the MySQL database
    host = "localhost"
    user = "root"
    password = os.getenv("MYSQL_PASSWORD")
    database = "container"

    # Create a connection
    connection = create_connection(host, user, password, database)

    # Get the email body
    if email_message.is_multipart():
        for part in email_message.walk():
            if part.get_content_type() == 'text/html':
                body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8')
                # print("\nBody:", body)
    else:
        body = email_message.get_payload(decode=True).decode(email_message.get_content_charset() or 'utf-8')
        # print("\nBody:", body)

    subject = email_message['Subject']
    vendor_email = re.findall(r'<(.*?)>', email_message['From'])
    received_date_temp = email_message['Date'].replace(" (GMT)", "")
    parsed_time = datetime.strptime(received_date_temp, "%a, %d %b %Y %H:%M:%S %z")
    received_date = parsed_time.strftime("%Y/%m/%d %H:%M:%S")
    current_datetime = datetime.now()
    created_date = current_datetime.strftime("%Y/%m/%d %H:%M:%S")

    soup = BeautifulSoup(body, 'html.parser')
    rows = soup.find_all('tr')

    with open('variable.json', 'r') as f:
        var_data = json.load(f)
    location_data = var_data['location_data']
    size_data = var_data['size_data']
    term_data = var_data['term_data']

    print(subject)
    print(vendor_email[0])
    # parse_html_content(body)

    match vendor_email[0]:
        # ---------------  Parsing for john@americanacontainers.com (John Rupert, Americana Containers Distribution Chain) --------------- #
        case "john@americanacontainers.com":
            clear_container_data(vendor_email[0])
            provider = "John Rupert, Americana Containers Distribution Chain"
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) >= 5 and cell_data[0] != "":
                    location = cell_data[0].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[1].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[2]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    feature, depot, eta = "", "", ""

                    quantity = cell_data[3].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[4].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                    if len(cell_data) > 5 and cell_data[5] != "":
                        feature = cell_data[5]

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for chris@americanacontainers.com (Chris Miller, Americana Containers Distribution Chain) --------------- #
        case "chris@americanacontainers.com":
            clear_container_data(vendor_email[0])
            provider = "Chris Miller, Americana Containers Distribution Chain"
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) >= 5 and cell_data[0] != "":
                    location = cell_data[0].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[1].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[2]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    eta = ""
                    feature, depot = cell_data[6], cell_data[5]
                    quantity = cell_data[3].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[4].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for tine@americanacontainers.com (Tine Patterson, Americana Containers Distribution Chain) --------------- #
        case "tine@americanacontainers.com":
            clear_container_data(vendor_email[0])
            provider = "Tine Patterson, Americana Containers Distribution Chain"
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) >= 5 and cell_data[0] != "":
                    location = cell_data[0].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[1].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[2]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    feature, depot, eta = "", "", ""
                    quantity = cell_data[3].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[4].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                    if len(cell_data) > 5 and cell_data[5] != "":
                        feature = cell_data[5]

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for jason@americanacontainers.com (Jason Clyde, Americana Containers Distribution Chain) --------------- #
        case "jason@americanacontainers.com":
            clear_container_data(vendor_email[0])
            provider = "Jason Clyde, Americana Containers Distribution Chain"
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) >= 5 and cell_data[0] != "":
                    location = cell_data[0].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[1].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[2]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    feature, depot, eta = cell_data[6], cell_data[5], ""
                    quantity = cell_data[3].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[4].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for johannes@oztradingltd.com (Johannes, OZ Trading Limited) --------------- #
        case "johannes@oztradingltd.com":
            clear_container_data(vendor_email[0])
            provider = "Johannes, OZ Trading Limited"
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) >= 7:
                    location = cell_data[3].split(",")[0].upper().replace("\n", "").replace("  ", " ").strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[1].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    feature, depot = "", ""
                    eta = "ETA: " + cell_data[4]
                    quantity = cell_data[2].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[6].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                    if "NEW" in cell_data[5]:
                        term = "1Trip"
                        feature = cell_data[5].replace("NEW", "").strip()
                    else:
                        term = "CW"
                else:
                    location = cell_data[2].split(",")[0].upper().replace("\n", "").replace("  ", " ").strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[0].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    feature, depot = "", ""
                    eta = "ETA: " + cell_data[3]
                    quantity = cell_data[1].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[5].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                    if "NEW" in cell_data[4]:
                        term = "1Trip"
                        feature = cell_data[4].replace("NEW", "").strip()
                    else:
                        term = "CW"

                insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for steven.gao@cgkinternational.com (Steven Gao, CGK International Limited) --------------- #
        case "steven.gao@cgkinternational.com":
            clear_container_data(vendor_email[0])
            provider = "Steven Gao, CGK International Limited"
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if cell_data[2].isdigit():
                    location = cell_data[1].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[0].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    depot, eta = "", ""
                    term = cell_data[5].replace(" ", "")
                    feature = cell_data[6] + ", YOM: " + cell_data[4]
                    quantity = cell_data[2].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[3].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for sales@isr-containers.com (Zarah Moore, ISR Containers) --------------- #
        case "sales@isr-containers.com":
            provider = "Zarah Moore, ISR Containers"
            if "SHIPPING CONTAINERS FOR SALE" in subject:
                clear_container_data(vendor_email[0])
                for i in range(1, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    location = cell_data[1].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[2].replace(" ", "").replace("'", "").replace("’", "")
                    for key, value in size_data.items():
                            if key == size:
                                size = value

                    term = cell_data[3]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    price, feature, depot, eta = 0, "", "", ""
                    quantity = cell_data[4].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for wayne.vandenburg@dutchcontainers.com (Wayne van den Burg, Dutch Container Merchants B.V.) --------------- #
        case "wayne.vandenburg@dutchcontainers.com":
            provider = "Wayne van den Burg, Dutch Container Merchants B.V."
            if "Arrival" in subject:
                clear_container_data(vendor_email[0])
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    location = cell_data[0].split(",")[0].upper().replace("\n", "").strip()
                    for key, value in location_data.items():
                            if key == location:
                                location = value

                    size = cell_data[2].replace(" ", "").replace("'", "").replace("\n", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[3].replace('\n', '')
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    price, depot = 0, ""
                    feature = cell_data[4].replace("\n", "")
                    eta = "ETA: " + cell_data[5].replace("\n", "")
                    quantity = cell_data[1].replace("\n", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            elif "Inventory" in subject:
                clear_container_data(vendor_email[0])
                for i in range(1, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    if cell_data[1].replace('\n', '').isdigit():
                        location = cell_data[0].split(",")[0].upper().replace("\n", "").strip()
                        for key, value in location_data.items():
                            if key == location:
                                location = value

                        size = cell_data[2].replace(" ", "").replace("'", "").replace("\n", "")
                        for key, value in size_data.items():
                            if key == size:
                                size = value

                        term = cell_data[3].replace('\n', '')
                        for key, value in term_data.items():
                            if key in term:
                                term = value

                        eta = ""
                        feature = cell_data[4].replace("\n", "")
                        depot= cell_data[5].replace('\n', '')
                        quantity = cell_data[1].replace("\n", "").strip()
                        quantity = int(quantity) if quantity.isdigit() else 1
                        price = cell_data[6].replace('\n', '').split(',')[0]
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for wayne.vandenburg@trident-containers.com (Wayne van den Burg, Trident Container Leasing B.V.) --------------- #
        case "wayne.vandenburg@trident-containers.com":
            provider = "Wayne van den Burg, Trident Container Leasing B.V."
            status = ""
            if "INVENTORY" in subject:
                clear_container_data(vendor_email[0])
                for i in range(1, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    if "ARRIVING" in cell_data[0]:
                        status = "ARRIVING"

                    if cell_data[1].replace("\n", "").isdigit():
                        location = cell_data[0].split(",")[0].upper().replace("\n", "").strip()
                        for key, value in location_data.items():
                            if key == location:
                                location = value

                        size = cell_data[2].replace(" ", "").replace("'", "").replace("\n", "")
                        for key, value in size_data.items():
                                if key == size:
                                    size = value

                        term = cell_data[3].replace("\n", "")
                        for key, value in term_data.items():
                            if key in term:
                                term = value

                        feature = cell_data[4].replace("\n", "")
                        quantity = cell_data[1].replace("\n", "").strip()
                        quantity = int(quantity) if quantity.isdigit() else 1
                        price = cell_data[6].replace("\n", "").split(",")[0]
                        price = int(price) if price.isdigit() else 0

                        if status:
                            depot = ""
                            eta = "ETA: " + cell_data[5].replace("\n", "")
                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)
                        else:
                            depot = cell_data[5].replace("\n", "")
                            eta = ""
                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for ryan@trident-containers.com (Ryan Garrido, Trident Container Leasing B.V.) --------------- #
        case "ryan@trident-containers.com":
            provider = "Ryan Garrido, Trident Container Leasing B.V."
            if "INVENTORY" in subject:
                clear_container_data(vendor_email[0])
                for i in range(1, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    if cell_data[1].replace("\n", "").isdigit():
                        location = cell_data[0].split(",")[0].upper().replace("\n", "").strip()
                        for key, value in location_data.items():
                                if key == location:
                                    location = value

                        size = cell_data[2].replace(" ", "").replace("'", "").replace("\n", "").replace("\r", "")
                        for key, value in size_data.items():
                            if key == size:
                                size = value

                        term = cell_data[3].replace("\n", "")
                        for key, value in term_data.items():
                            if key in term:
                                term = value

                        feature, depot, eta = cell_data[4].replace("\n", ""), cell_data[5].replace("\n", ""), ""
                        quantity = cell_data[1].replace("\n", "").strip()
                        quantity = int(quantity) if quantity.isdigit() else 1
                        price = cell_data[6].replace("\n", "").split(",")[0]
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for e4.mevtnhrict@gcc2011.com (Oliver Egonio, Global Container & Chassis) --------------- #
        case "e4.mevtnhrict@gcc2011.com":
            provider = "Oliver Egonio, Global Container & Chassis"
            if "Updated" in subject:
                clear_container_data(vendor_email[0])
                sizes = rows[0].find_all('td')
                size_list = [size.get_text() for size in sizes]

                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    location = cell_data[1].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    term = cell_data[3]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    feature, depot, eta = "", "", ""
                    grade = cell_data[2].replace("DC", "").replace("'", "")

                    for j in range(4, len(cell_data), 2):
                        if j+1 < len(cell_data) and cell_data[j] and cell_data[j+1]:
                            size = size_list[int(j / 2) + 2]
                            size = size.replace(" ", "").replace("'", "")
                            for key, value in size_data.items():
                                if key == size:
                                    size = value

                            if grade:
                                size = size + " " + grade.upper()

                            quantity = cell_data[j].replace("+", "").strip()
                            quantity = int(quantity) if quantity.isdigit() else 1
                            price = cell_data[j+1].replace("$", "").replace(",", "").strip()
                            price = int(price) if price.isdigit() else 0

                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for ash@container-xchange.com (Ashish Sharma, XChange) --------------- #
        case "ash@container-xchange.com":
            clear_container_data(vendor_email[0])
            provider = "Ashish Sharma, XChange"
            size_list = ["20'", "20'", "20'", "20'", "40'HC", "40'HC", "40'HC", "40'HC",]
            grade_list = ['', '', 'Double Door', 'Side Door', '', '', 'Double Door', 'Side Door']
            term_list = ['Cargo Worthy', '1Trip', '1Trip', '1Trip', 'Cargo Worthy', '1Trip', '1Trip', '1Trip']
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]
                if "\n\xa0\n" not in cell_data[0]:
                    location = cell_data[0].split(",")[0].upper().replace("\n", "").strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    for j in range(1, len(cell_data)):
                        grade = grade_list[j-1]
                        size = size_list[j-1].replace(" ", "").replace("'", "")
                        for key, value in size_data.items():
                            if key == size:
                                size = value

                        if grade:
                            size = size + " " + grade.upper()

                        term = term_list[j-1]
                        for key, value in term_data.items():
                            if key in term:
                                term = value

                        feature, depot, eta = "", "", ""
                        quantity = 1
                        price = cell_data[j].replace("$", "").replace(",", "").strip().replace("\n", "")
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for Saquib.amiri@sadecontainers.com (Saquib Amiri, SADE Containers GmbH) --------------- #
        case "Saquib.amiri@sadecontainers.com":
            provider = "Saquib Amiri, SADE Containers GmbH"
            if "Inventory" in subject:
                clear_container_data(vendor_email[0])
                for i in range(1, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    location = cell_data[0].split(",")[0].upper().replace("\n", "").strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size_temp = cell_data[1].replace("\n", "").split(' ')[0] + " "
                    size = size_temp.replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[1].replace("\n", "").replace(size_temp, "")
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    feature, eta = "", ""
                    depot = cell_data[2].replace("\n", "")

                    quantity = cell_data[3].replace("\n", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[4].replace("$", "").replace(",", "").strip().replace("\n", "")
                    price = int(price) if price.isdigit() else 0

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for JAnguish@ism247.com (Jack Anguish, ISM) --------------- #
        case "JAnguish@ism247.com":
            provider = "Jack Anguish, ISM"
            if "Inventory" in subject:
                clear_container_data(vendor_email[0])
                location, status = "", ""
                for i in range(1, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]
                    if len(cell_data) == 0 or len(cell_data) == 1 or len(cell_data) == 4 or len(cell_data) == 7:
                        i += 1

                    if len(cell_data) == 2 and "Available" in cell_data[1]:
                        if "Location" in cell_data[0]:
                            i += 1
                        else:
                            location = cell_data[0].split(",")[0].upper().replace("\n", "").replace(" ", "").strip()
                            for key, value in location_data.items():
                                if key == location:
                                    location = value

                    elif len(cell_data) == 5:
                        location = cell_data[0].split(",")[0].upper().replace("\n", "").replace(" ", "").strip()
                        for key, value in location_data.items():
                            if key == location:
                                location = value

                        status = cell_data[4].replace("\n", "").replace("\xa0", "")

                    if len(cell_data) == 6 and status == "Price" and cell_data[0].replace("\n", "") != "":
                        size = cell_data[1].replace(" ", "").replace("'", "").replace("\n", "").replace("\r", "").replace("\xa0", "")
                        for key, value in size_data.items():
                            if key == size:
                                size = value

                        term = cell_data[2].replace("\n", "").replace("\r", "").replace("\xa0", "")
                        for key, value in term_data.items():
                            if key in term:
                                term = value

                        depot, eta = "", ""
                        feature = cell_data[3].replace("\n", "").replace("\r", "").replace("\xa0", "") + ", " + cell_data[4].replace("\n", "").replace("\r", "").replace("\xa0", "")
                        feature = feature.replace("N, N", "")

                        quantity = cell_data[0].replace("\n", "").replace("\r", "").strip()
                        quantity = int(quantity) if quantity.isdigit() else 1
                        price = cell_data[5].replace("$", "").replace(",", "").replace("\n", "").replace("\r", "").replace("\xa0", "").split("(")[0].strip()
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                    if len(cell_data) == 6 and status == "ETA" and cell_data[0].replace("\n", "") != "":
                        size = cell_data[1].replace(" ", "").replace("'", "").replace("\n", "").replace("\r", "").replace("\xa0", "")
                        for key, value in size_data.items():
                            if key == size:
                                size = value

                        term = cell_data[2].replace("\n", "").replace("\r", "").replace("\xa0", "")
                        for key, value in term_data.items():
                            if key in term:
                                term = value

                        price, depot = 0, ""
                        eta = cell_data[5].replace("\n", "").replace("\r", "").replace("\xa0", "")
                        feature = cell_data[3].replace("\n", "").replace("\r", "").replace("\xa0", "") + ", " + cell_data[4].replace("\n", "").replace("\r", "").replace("\xa0", "")
                        feature = feature.replace("N, N", "")
                        quantity = cell_data[0].replace("\n", "").replace("\r", "").strip()
                        quantity = int(quantity) if quantity.isdigit() else 1

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for sales@tritoncontainersales.com (TRITON Container Sales) --------------- #
        case "sales@tritoncontainersales.com":
            clear_container_data(vendor_email[0])
            provider = "TRITON Container Sales"
            for i in range(6, len(rows)-2):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                location = cell_data[0].split(",")[0].upper().replace("\n", "").replace("\r", "").strip()
                for key, value in location_data.items():
                    if key == location:
                        location = value

                term = "CW"
                quantity, size, feature, depot, eta = 1,  "", "", "", ""
                price_reefer, price_tripped = 0, 0
                if cell_data[1] != "-":
                    price_reefer = int(cell_data[1].replace("$", "").replace(",", "").split("+")[0].strip())
                if cell_data[2] != "-":
                    price_tripped = int(cell_data[2].replace("$", "").replace(",", "").split("+")[0].strip())

                insert_container_record(connection, size, quantity, term, location, price_reefer, feature, depot, eta, provider, vendor_email[0], received_date, created_date)
                insert_container_record(connection, size, quantity, term, location, price_tripped, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for thomas@fulidacontainer.com (Thomas, Fulida Container Limited) --------------- #
        case "thomas@fulidacontainer.com":
            clear_container_data(vendor_email[0])
            provider = "Thomas, Fulida Container Limited"
            for i in range(0, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if cell_data[0] != "LOCATION" and cell_data[0] != "" and cell_data[1] != "\u3000":
                    location = cell_data[0].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    cell_data[1] = cell_data[1].replace("（", "(")
                    size = cell_data[1].replace(" ", "").replace("'", "").split("(")[0]
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[2]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    feature = cell_data[3] + " " + cell_data[4]
                    depot = cell_data[5]
                    eta = "ETA: " + cell_data[8]

                    quantity = cell_data[6].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[7].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for magui.cheung@northatlanticcontainer.com (ThomaMagui Cheungs, Account Management Associate) --------------- #
        case "magui.cheung@northatlanticcontainer.com":
            clear_container_data(vendor_email[0])
            provider = "ThomaMagui Cheungs, Account Management Associate"
            size_list = ["20' STD", "20 STD", "40' STD", "40 STD", "20' HC", "20 HC", "40' HC", "40 HC", "45' HC", "45 HC", "53' HC", "53 HC"]
            grade_list = ["DOUBLE DOOR", "OPEN SIDE", "DUOCON", "FLAT RACK", "OPEN TOP"]
            eta_list = ["ARRIVING", "GATEBUY", "FOR PICK UP ASAP", "TERMINAL"]
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) > 3:
                    location = cell_data[0].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    quantity = cell_data[2].replace("-", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[3].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0
                    item = cell_data[1]

                if len(cell_data) == 3:
                    quantity = cell_data[1].replace("-", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[2].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0
                    item = cell_data[0]

                size, term, grade, feature, depot, eta = "", "", "", "", "", ""

                for size_value in size_list:
                    if size_value in item:
                        size = size_value
                        break
                size = size.replace(" ", "").replace("'", "")
                for key, value in size_data.items():
                        if key == size:
                            size = value
                for grade_value in grade_list:
                    if grade_value in item:
                        grade = grade_value
                        break
                if grade:
                    size = size + " " + grade

                for key, value in term_data.items():
                    if key in item:
                        term = value

                for eta_value in eta_list:
                    if eta_value in item:
                        eta = eta_value
                        break

                if "(" in item and ")" in item and "RAL" in item:
                    feature = item.split("(")[1].split(")")[0]
                    if "RAL" not in feature:
                        feature = item.split(")")[1].split("(")[1].split(")")[0]

                insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for jeff@lummid.com (Jeff Young, Lummid Containers) --------------- #
        case "jeff@lummid.com":
            clear_container_data(vendor_email[0])
            provider = "Jeff Young, Lummid Containers"
            location = ""
            for row in rows:
                cells = row.find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) == 4 and "Market" in cell_data[0]:
                    location = "USA"

                if location and cell_data.count("\xa0") < 4 and len(cell_data) > 3 and "@" in cell_data[3]:
                    if "\xa0" not in cell_data[0]:
                        location = cell_data[0].split(",")[0].upper().strip()
                        for key, value in location_data.items():
                            if key == location:
                                location = value

                    size = cell_data[1].split(" ")[0]
                    term = cell_data[1].replace(size, "").strip()
                    size = size.replace("'", "").replace(" ", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    if "ST" in term:
                        term = term.replace("ST", "").strip()

                    grade_list = {"D.D." : "DOUBLE DOOR", "S.D." : "SIDE DOOR", "O.S." : "OPEN SIDE"}
                    grade, eta = "", ""
                    for key, value in grade_list.items():
                        if key in term:
                            grade = value
                            term = term.replace(key, "").strip()

                    if grade:
                        size = size + " " + grade

                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    if "$" in cell_data[2]:
                        quantity = cell_data[2].split("x")[0].strip()
                        quantity = int(quantity) if quantity.isdigit() else 1
                        price = cell_data[2].split("x")[1].replace("$", "").strip()
                        price = int(price) if price.isdigit() else 0
                    else:
                        quantity, price = cell_data[2].strip(), 0
                        quantity = int(quantity) if quantity.isdigit() else 1

                    feature, depot = cell_data[3].split("@")[0], cell_data[3].split("@")[1]

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for mjs@lummid.com (Michael Stangel, Lummid Containers) --------------- #
        case "mjs@lummid.com":
            clear_container_data(vendor_email[0])
            provider = "Michael Stangel, Lummid Containers"
            location = ""
            for row in rows:
                cells = row.find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) == 6 and "Location" in cell_data[0]:
                    location = "USA"

                if location and len(cell_data) > 5 and cell_data[5].isdigit():

                    location = cell_data[0].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[1].replace(" ", "").replace("'", "").split("-")[0]
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term, depot = "", ""
                    feature = cell_data[4]
                    eta = "ETA: " + cell_data[2]
                    quantity = cell_data[5].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[3].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for eastcoast@lummid.com (Jeff Young, Lummid Containers) --------------- #
        case "eastcoast@lummid.com":
            clear_container_data(vendor_email[0])
            provider = "Jeff Young, Lummid Containers"
            location = ""
            for row in rows:
                cells = row.find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) == 4 and "Market" in cell_data[0]:
                    location = "USA"

                if location and cell_data.count("\xa0") < 4 and len(cell_data) > 3 and "@" in cell_data[3]:
                    if "\xa0" not in cell_data[0]:
                        location = cell_data[0].split(",")[0].upper().strip()
                        for key, value in location_data.items():
                            if key == location:
                                location = value

                    size = cell_data[1].split(" ")[0]
                    term = cell_data[1].replace(size, "").strip()
                    size = size.replace("'", "").replace(" ", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    if "ST" in term:
                        term = term.replace("ST", "").strip()

                    grade_list = {"D.D." : "DOUBLE DOOR", "S.D." : "SIDE DOOR", "O.S." : "OPEN SIDE"}
                    grade, eta = "", ""
                    for key, value in grade_list.items():
                        if key in term:
                            grade = value
                            term = term.replace(key, "").strip()

                    if grade:
                        size = size + " " + grade

                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    if "$" in cell_data[2]:
                        quantity = cell_data[2].split("x")[0].strip()
                        quantity = int(quantity) if quantity.isdigit() else 1
                        price = cell_data[2].split("x")[1].replace("$", "").strip()
                        price = int(price) if price.isdigit() else 0
                    else:
                        quantity, price = cell_data[2].strip(), 0
                        quantity = int(quantity) if quantity.isdigit() else 1

                    feature, depot = cell_data[3].split("@")[0], cell_data[3].split("@")[1]

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for westcoast@lummid.com (Daniel Callaway, Lummid Containers) --------------- #
        case "westcoast@lummid.com":
            clear_container_data(vendor_email[0])
            provider = "Daniel Callaway, Lummid Containers"
            location = ""
            for row in rows:
                cells = row.find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) <= 7 and len(cell_data) >= 5 and "Market" in cell_data[0]:
                    location = "CANADA"

                if location and cell_data.count("\xa0") < 5 and len(cell_data) > 4 and len(cell_data[2]) < 3:
                    if "\xa0" not in cell_data[0]:
                        location = cell_data[0].split("@")[0].split(",")[0].upper().strip()
                        for key, value in location_data.items():
                            if key == location:
                                location = value

                    size = cell_data[1].split(" ")[0] + cell_data[1].split(" ")[1]
                    size = size.replace("'", "").replace(" ", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[1].split(" ")[-1].strip()
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    depot, eta = "", ""
                    feature = cell_data[4].replace("\xa0", "")
                    quantity = cell_data[2].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[3].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for ryanchoi@muwon.com (Ryan Jongwon Choi, Muwon USA) --------------- #
        case "ryanchoi@muwon.com":
            clear_container_data(vendor_email[0])
            provider = "Ryan Jongwon Choi, Muwon USA"
            sizes = rows[1].find_all('td')
            size_list = [size.get_text() for size in sizes]
            term_list = ["CW", "CW", "CW", "1Trip", "1Trip"]
            for i in range(2, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                feature, depot = "", ""
                if len(cell_data) > 5:
                    location = cell_data[1].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value
                    depot = cell_data[2] if len(cell_data) > 7 else ""

                    start_index = 3 if len(cell_data) > 7 else 2
                    for j in range(start_index, len(cell_data)):
                        if "\xa0" not in cell_data[j]:
                            quantity = cell_data[j].replace("(", "").replace(")", "").replace("$", "").strip()
                            quantity = int(quantity) if quantity.isdigit() else 1
                            eta = "GATE BUY Available" if "(" in cell_data[j] else ""
                            size = size_list[j].replace(" ", "").replace("'", "")
                            for key, value in size_data.items():
                                if key == size:
                                    size = value
                            term = term_list[j-start_index]
                            price = 0

                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for erica@icc-solution.com (Erica Medina, International Container & Chassis Solution) --------------- #
        case "erica@icc-solution.com":
            clear_container_data(vendor_email[0])
            provider = "Erica Medina, International Container & Chassis Solution"
            for i in range(2, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) == 6:
                    location = cell_data[0].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[1].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[2]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    depot = cell_data[4]
                    quantity = cell_data[3].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[5].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                if len(cell_data) == 5:
                    size = cell_data[0].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[1]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    depot = cell_data[3]
                    quantity = cell_data[2].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[4].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                if len(cell_data) == 4:
                    size = cell_data[0].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[1]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    quantity = cell_data[2].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[3].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                if len(cell_data) >= 4:
                    feature, eta = "", ""
                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for jenny@icc-solution.com (Jenny Roberts, International Container & Chassis Solution) --------------- #
        case "jenny@icc-solution.com":
            clear_container_data(vendor_email[0])
            provider = "Jenny Roberts, International Container & Chassis Solution"
            for i in range(2, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) == 6:
                    location = cell_data[0].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    size = cell_data[1].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[2]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    depot = cell_data[4]
                    quantity = cell_data[3].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[5].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                if len(cell_data) == 5:
                    size = cell_data[0].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[1]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    depot = cell_data[3]
                    quantity = cell_data[2].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[4].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                if len(cell_data) == 4:
                    size = cell_data[0].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term = cell_data[1]
                    for key, value in term_data.items():
                        if key in term:
                            term = value

                    quantity = cell_data[2].replace("+", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = cell_data[3].replace("$", "").replace(",", "").strip()
                    price = int(price) if price.isdigit() else 0

                if len(cell_data) >= 4:
                    feature, eta = "", ""
                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for olaf@marinecw.com (Marine Container World) --------------- #
        case "olaf@marinecw.com":
            clear_container_data(vendor_email[0])
            provider = "Marine Container World"
            for row in rows:
                cells = row.find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if cell_data[0] != "LOCATION" and cell_data[0] != "":
                    if cell_data[0] != "\xa0":
                        location = cell_data[0].split(",")[0].upper().strip()
                        for key, value in location_data.items():
                            if key == location:
                                location = value

                    size = cell_data[1].replace(" ", "").replace("'", "")
                    if "x" in size:
                        quantity = size.split("x")[0].strip()
                        quantity = int(quantity) if quantity.isdigit() else 1
                        size = size.split("x")[1].strip()
                    else:
                        quantity = 1
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    term, feature, depot, eta, price = "", "", "", "", 0
                    if "1-trip" in cell_data[2] or "NEW" in cell_data[2]:
                        term = "1Trip"
                    if "Used" in cell_data[2] or "Cargo Worthy" in cell_data[2]:
                        term = "CW"
                    if len(cell_data[2].split(",")) > 2:
                        depot = cell_data[2].split(",")[2].strip()

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for yansen@megaconusa.com (Yansen C. LO, MEGA Container Sales) --------------- #
        case "yansen@megaconusa.com":
            clear_container_data(vendor_email[0])
            provider = "Yansen C. LO, MEGA Container Sales"
            sizes = rows[2].find_all('td')
            size_list = [size.get_text() for size in sizes]
            for i in range(3, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if cell_data[0] != "":
                    location = cell_data[0].split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                    feature, depot, eta, price = "", "", "", 0
                    for j in range(1, len(cell_data) - 1):
                        if cell_data[j]:
                            size = size_list[j-1]
                            size = size.replace(" ", "").replace("'", "")
                            for key, value in size_data.items():
                                if key == size:
                                    size = value

                            term = "Used" if j > 3 else "1Trip"
                            quantity = cell_data[j].replace("+", "").strip()
                            quantity = int(quantity) if quantity.isdigit() else 1

                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for wayneterry@florens.com (Wayne Terry, Florens Asset Management) --------------- #
        case "wayneterry@florens.com":
            clear_container_data(vendor_email[0])
            provider = "Wayne Terry, Florens Asset Management"
            sizes = rows[1].find_all('td')
            size_list = [size.get_text() for size in sizes]
            for i in range(2, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                location = cell_data[1].split(",")[0].replace("\n", "").upper().strip()
                for key, value in location_data.items():
                    if key == location:
                        location = value

                depot = cell_data[2].replace("\n", "")
                feature, eta, price = "", "", 0
                for j in range(3, len(cell_data)):
                    if "\xa0" not in cell_data[j]:
                        size = size_list[j-3]
                        size = size.replace(" ", "").replace("'", "").replace(".", "").replace("\n", "")
                        for key, value in size_data.items():
                            if key == size:
                                size = value

                        term = "WWT"
                        quantity = cell_data[j].replace("+", "").strip()
                        quantity = int(quantity) if quantity.isdigit() else 1

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for equipment@conwaycs.com (Margarita Kolecenko, Conway) --------------- #
        case "equipment@conwaycs.com":
            clear_container_data(vendor_email[0])
            provider = "Margarita Kolecenko, Conway"
            for row in rows:
                cells = row.find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) == 4 and cell_data[3].isdigit():
                    if cell_data[0] == "USA" or cell_data[0] == "CANADA":
                        location = cell_data[1].split("(")[0].upper().strip()
                        for key, value in location_data.items():
                            if key == location:
                                location = value

                        size = cell_data[2].replace(" ", "").replace("'", "")
                        for key, value in size_data.items():
                            if key == size:
                                size = value

                        term, feature, depot, eta, price = "", "", "", "", 0
                        quantity = cell_data[3].replace("+", "").strip()
                        quantity = int(quantity) if quantity.isdigit() else 1

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

    # Close the connection
    if connection:
        connection.close()

    return body

def get_message_content_plain(service, message_id):
    """Retrieve a specific message by its ID and decode its content."""
    message = service.users().messages().get(userId='me', id=message_id, format='raw').execute()
    msg_raw = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
    email_message = message_from_bytes(msg_raw)

    # Replace the following variables with your database credentials
    host = "localhost"
    user = "root"
    password = os.getenv("MYSQL_PASSWORD")
    database = "container"

    # Create a connection
    connection = create_connection(host, user, password, database)

    # Get the email body
    if email_message.is_multipart():
        for part in email_message.walk():
            if part.get_content_type() == 'text/plain':
                body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8')
                # print("\nBody:", body)
    else:
        body = email_message.get_payload(decode=True).decode(email_message.get_content_charset() or 'utf-8')
        # print("\nBody:", body)

    vendor_email = re.findall(r'<(.*?)>', email_message['From'])
    subject = email_message['Subject']
    received_date_temp = email_message['Date']
    parsed_time = datetime.strptime(received_date_temp, "%a, %d %b %Y %H:%M:%S %z")
    received_date = parsed_time.strftime("%Y/%m/%d %H:%M:%S")
    current_datetime = datetime.now()
    created_date = current_datetime.strftime("%Y/%m/%d %H:%M:%S")
    content = re.sub(r"^\s*$\n", "", body, flags=re.MULTILINE)

    with open('variable.json', 'r') as f:
        var_data = json.load(f)
    location_data = var_data['location_data']
    size_data = var_data['size_data']
    term_data = var_data['term_data']

    print(subject)
    print(vendor_email[0])
    # print(content)

    match vendor_email[0]:
        # ---------------  Parsing for rolly@oceanbox.cn (Rolly, Oceanbox logistic limited) --------------- #
        case "rolly@oceanbox.cn":
            provider = "Rolly, Oceanbox logistic limited"
            if "inventory" in subject:
                clear_container_data(vendor_email[0])
                content_data = content.split("Thank you!")[1].split("Container expert from China")[0].split("Note")[0].split("\n")
                location = ''
                for i in range(0, len(content_data)):
                    if len(content_data[i].split(",")) < 3:
                        location = content_data[i].split(",")[0].upper().strip()
                        for key, value in location_data.items():
                            if key == location:
                                location = value
                    else:
                        content_data[i] = content_data[i].replace("，", ",")
                        feature, depot, eta, term = "", "", "", ""
                        quantity = content_data[i].split(" x ")[0].strip()
                        quantity = int(quantity) if quantity.isdigit() else 1
                        size = content_data[i].split(",")[0].split(" x ")[1]
                        for key, value in size_data.items():
                                    if key == size:
                                        size = value

                        if len(content_data[i].split(",")) == 5:
                            if "$" in content_data[i].split(",")[3] and "," in content_data[i].split("$")[1]:
                                term = content_data[i].split(",")[1].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                if "ETA" in content_data[i].split(",")[4]:
                                    eta = content_data[i].split(",")[4].strip()
                                else:
                                    depot = content_data[i].split(",")[4].strip()
                                feature = content_data[i].split(",")[2].strip()

                                if "gatebuy" in content_data[i].split(",")[3] and depot == "":
                                    depot = "gatebuy"
                                price = content_data[i].split(",")[3].split("$")[1].strip()
                                price = int(price) if price.isdigit() else 0

                                insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                            if "$" not in content_data[i]:
                                term = content_data[i].split(",")[2].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                if "ETA" in content_data[i].split(",")[4]:
                                    eta = content_data[i].split(",")[4].strip()
                                else:
                                    depot = content_data[i].split(",")[4].strip()
                                feature = content_data[i].split(",")[3].strip()

                                insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                        if len(content_data[i].split(",")) == 6 and "$" in content_data[i].split(",")[3] and "," in content_data[i].split("$")[1]:
                            term = content_data[i].split(",")[1].strip()
                            for key, value in term_data.items():
                                if key in term:
                                    term = value

                            if content_data[i].count("$") == 3:
                                depot = content_data[i].split(",")[2].strip()

                                if "gatebuy" in content_data[i].split(",")[3]:
                                    depot = "gatebuy"
                                price = content_data[i].split(",")[3].split("$")[1].strip()
                                price = int(price) if price.isdigit() else 0

                                insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                            else:
                                if "Full open side" in content_data[i].split(",")[4]:
                                    depot = content_data[i].split(",")[5].strip()
                                else:
                                    depot = content_data[i].split(",")[4].strip().replace("(", "")
                                    eta = content_data[i].split(",")[5].strip().replace(")", "")

                                feature = content_data[i].split(",")[2].strip()

                                if "gatebuy" in content_data[i].split(",")[3] and depot == "":
                                    depot = "gatebuy"
                                price = content_data[i].split(",")[3].split("$")[1].strip()
                                price = int(price) if price.isdigit() else 0

                                insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                        if len(content_data[i].split(",")) == 6 and content_data[i].count("$") == 1 and "$" in content_data[i].split(",")[4] and "," in content_data[i].split("$")[1]:
                            if "door" in content_data[i].split(",")[1] or "full open side" in content_data[i].split(",")[1]:
                                term = content_data[i].split(",")[2].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                feature = content_data[i].split(",")[3].strip()
                                depot = content_data[i].split(",")[5].strip()

                            elif "door" in content_data[i].split(",")[2] or "full open side" in content_data[i].split(",")[2]:
                                term = content_data[i].split(",")[1].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                feature = content_data[i].split(",")[3].strip()
                                depot = content_data[i].split(",")[5].strip()
                            else:
                                if "ETA" in content_data[i].split(",")[5]:
                                    eta = content_data[i].split(",")[5].strip()
                                    depot = content_data[i].split(",")[3].strip()
                                elif "ETA" in content_data[i].split(",")[3]:
                                    eta = content_data[i].split(",")[3].strip()
                                    depot = content_data[i].split(",")[5].strip()
                                else:
                                    depot = content_data[i].split(",")[5].strip()

                                if "full open side" in content_data[i]:
                                    depot = content_data[i].split(",")[3].strip()

                                term = content_data[i].split(",")[1].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                feature = content_data[i].split(",")[2].strip()

                            if "YOM" in content_data[i]:
                                term = content_data[i].split(",")[1].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                feature = content_data[i].split(",")[2].strip() + "," + content_data[i].split(",")[3]
                                depot = content_data[i].split(",")[5].strip()

                            if "gatebuy" in content_data[i].split(",")[4] and depot == "":
                                depot = "gatebuy"

                            price = content_data[i].split(",")[4].split("$")[1].strip()
                            price = int(price) if price.isdigit() else 0

                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                        if len(content_data[i].split(",")) >= 7:
                            if "$" in content_data[i].split(",")[3]:
                                if len(content_data[i].split(",")) == 7:
                                    depot = content_data[i].split(",")[4].strip() + "," + content_data[i].split(",")[5]
                                    eta = content_data[i].split(",")[6].strip()

                                if len(content_data[i].split(",")) == 9:
                                    depot = content_data[i].split(",")[4].strip() + "," + content_data[i].split(",")[5] + "," + content_data[i].split(",")[6] + "," + content_data[i].split(",")[7]
                                    eta = content_data[i].split(",")[8].strip()

                                term = content_data[i].split(",")[1].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                feature = content_data[i].split(",")[2].strip()

                                if "gatebuy" in content_data[i].split(",")[3] and depot == "":
                                    depot = "gatebuy"

                                price = content_data[i].split(",")[3].split("$")[1].strip()
                                price = int(price) if price.isdigit() else 0

                                insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                            if "$" in content_data[i].split(",")[4]:
                                if "door" in content_data[i] or "full open side" in content_data[i]:
                                    term = content_data[i].split(",")[2].strip()
                                    for key, value in term_data.items():
                                        if key in term:
                                            term = value

                                    feature = content_data[i].split(",")[3]
                                    depot = content_data[i].split(",")[5].strip() + "," + content_data[i].split(",")[6]
                                else:
                                    term = content_data[i].split(",")[1].strip()
                                    for key, value in term_data.items():
                                        if key in term:
                                            term = value

                                    feature = content_data[i].split(",")[2].strip()
                                    depot = content_data[i].split(",")[3].strip() + "," + content_data[i].split(",")[5] + "," + content_data[i].split(",")[6]

                                if "gatebuy" in content_data[i].split(",")[4] and depot == "":
                                    depot = "gatebuy"

                                price = content_data[i].split(",")[4].split("$")[1].strip()
                                price = int(price) if price.isdigit() else 0

                                insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                            if "$" in content_data[i].split(",")[6]:
                                if "door" in content_data[i] or "full open side" in content_data[i]:
                                    term = content_data[i].split(",")[3].strip()
                                    for key, value in term_data.items():
                                        if key in term:
                                            term = value

                                    feature = content_data[i].split(",")[4].strip()
                                    depot = content_data[i].split(",")[5].strip()
                                else:
                                    term = content_data[i].split(",")[1].strip()
                                    for key, value in term_data.items():
                                        if key in term:
                                            term = value

                                    feature = content_data[i].split(",")[2].strip() + "," + content_data[i].split(",")[3]
                                    depot = content_data[i].split(",")[4].strip() + "," + content_data[i].split(",")[5]

                                if "gatebuy" in content_data[i].split(",")[6] and depot == "":
                                    depot = "gatebuy"

                                price = content_data[i].split(",")[6].split("$")[1].strip()
                                price = int(price) if price.isdigit() else 0

                                insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                        if "$" in content_data[i].split(",")[2]:
                            if "RAL" in content_data[i]:
                                depot = content_data[i].split(",")[3].strip()
                                feature = content_data[i].split(",")[1].strip()
                                price = int(content_data[i].split(",")[2].replace("$", '').strip())
                            else:
                                term = content_data[i].split(",")[1].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                if "ETA" in content_data[i].split(",")[3]:
                                    eta = content_data[i].split(",")[3].strip()
                                else:
                                    depot = content_data[i].split(",")[3].strip()

                                if len(content_data[i].split(",")) == 5:
                                    depot = content_data[i].split(",")[3] + "," + content_data[i].split(",")[4]
                                    depot = depot.strip()

                                if "gatebuy" in content_data[i].split(",")[2] and depot == "":
                                    depot = "gatebuy"

                                price = content_data[i].split(",")[2].split("$")[1].strip()
                                price = int(price) if price.isdigit() else 0

                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                        if len(content_data[i].split(",")) == 4 and "$" in content_data[i].split(",")[3] and content_data[i].count("$") == 1:
                            term = content_data[i].split(",")[1].strip()
                            for key, value in term_data.items():
                                if key in term:
                                    term = value

                            feature = content_data[i].split(",")[2].strip()

                            if "gatebuy" in content_data[i].split(",")[3] and depot == "":
                                    depot = "gatebuy"

                            price = content_data[i].split(",")[3].split("$")[1].strip()
                            price = int(price) if price.isdigit() else 0

                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                        if len(content_data[i].split(",")) == 5 and "$" in content_data[i].split(",")[4] and content_data[i].count("$") == 1:
                            if "door" in content_data[i] or "full open side" in content_data[i]:
                                if "RAL" in content_data[i].split(",")[2]:
                                    feature = content_data[i].split(",")[2].strip()
                                    depot = content_data[i].split(",")[3].strip()
                                else:
                                    term = content_data[i].split(",")[2].strip()
                                    for key, value in term_data.items():
                                        if key in term:
                                            term = value

                                    if "ETA" in content_data[i].split(",")[3]:
                                        feature = content_data[i].split(",")[3].split("ETA")[0].strip()
                                        eta = "ETA" + content_data[i].split(",")[3].split("ETA")[1].strip()
                                    else:
                                        feature = content_data[i].split(",")[3].strip()
                            else:
                                if "ETA" in content_data[i].split(",")[3]:
                                    eta = content_data[i].split(",")[3].strip()
                                else:
                                    depot = content_data[i].split(",")[3].strip()

                                term = content_data[i].split(",")[1].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value
                                feature = content_data[i].split(",")[2].strip()

                            if "gatebuy" in content_data[i].split(",")[4] and depot == "":
                                    depot = "gatebuy"

                            price = content_data[i].split(",")[4].split("$")[1].strip()
                            price = int(price) if price.isdigit() else 0

                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                        if len(content_data[i].split(",")) == 6 and "$" in content_data[i].split(",")[5] and content_data[i].count("$") == 1:
                            if "door" in content_data[i] or "full open side" in content_data[i]:
                                term = content_data[i].split(",")[2].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                feature = content_data[i].split(",")[3].strip()
                                depot = content_data[i].split(",")[4].strip()
                            else:
                                term = content_data[i].split(",")[1].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value
                                feature = content_data[i].split(",")[2].strip() + "," + content_data[i].split(",")[3]
                                depot = content_data[i].split(",")[4].strip()

                            if "gatebuy" in content_data[i].split(",")[5] and depot == "":
                                    depot = "gatebuy"

                            price = content_data[i].split(",")[5].split("$")[1].strip()
                            price = int(price) if price.isdigit() else 0

                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                        if "$" not in content_data[i]:
                            if "door" in content_data[i] or "full open side" in content_data[i]:
                                term = content_data[i].split(",")[2].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                feature = content_data[i].split(",")[3].strip()
                                depot = content_data[i].split(",")[4].strip()
                            else:
                                term = content_data[i].split(",")[1].strip()
                                for key, value in term_data.items():
                                    if key in term:
                                        term = value

                                feature = content_data[i].split(",")[2].strip()
                                depot = content_data[i].split(",")[3].strip()

                            price = 0

                            insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for Bryan@scontainers.com (Bryan Lucas, Star Container Solution) --------------- #
        case "Bryan@scontainers.com":
            clear_container_data(vendor_email[0])
            provider = "Bryan Lucas, Star Container Solution"
            content_data = content.split("\n")
            location = ''
            for i in range(0, len(content_data)):
                if "*" in content_data[i] and "$" not in content_data[i] and "ETA" not in content_data[i]:
                    location = content_data[i].replace("*", "").split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value

                if "$" in content_data[i]:
                    feature, depot, eta, = "", "", ""
                    quantity = content_data[i].replace("*", "").split("X")[0].strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    if "CW" in content_data[i]:
                        size = content_data[i].replace("*", "").split("X")[1].split("CW")[0].replace("'", "").replace(" ", "")
                        for key, value in size_data.items():
                            if key == size:
                                size = value
                        feature = content_data[i].split(" - ")[0].split("CW")[1]
                        term = "CW"
                        price = content_data[i].replace("*", "").split(" - ")[1].replace("$", "").replace(",", "").replace("EACH", "").strip()
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                    elif "ONE TRIP" in content_data[i]:
                        if "OPEN SIDE" in content_data[i]:
                            size = content_data[i].replace("*", "").split("X")[1].split("OPEN SIDE")[0].replace("'", "").replace(" ", "")
                            for key, value in size_data.items():
                                if key == size:
                                    size = value
                            size = size + " OPEN SIDE"
                        else:
                            size = content_data[i].replace("*", "").split("X")[1].split("ONE TRIP")[0].replace("'", "").replace(" ", "")
                            for key, value in size_data.items():
                                if key == size:
                                    size = value

                        term = "1Trip"
                        feature = content_data[i].split("(")[1].split(")")[0]
                        price = content_data[i].replace("*", "").split(" - ")[1].replace("$", "").replace(",", "").replace("EACH", "").strip()
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                    elif "IICL" in content_data[i]:
                        feature = content_data[i].split("(")[1].split(")")[0]
                        size = content_data[i].replace("*", "").split("X")[1].split("IICL")[0].replace("'", "").replace(" ", "")
                        for key, value in size_data.items():
                            if key == size:
                                size = value
                        term = "IICL"
                        price = content_data[i].replace("*", "").split(" - ")[1].replace("$", "").replace(",", "").replace("EACH", "").strip()
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                    elif "AS IS" in content_data[i] or "WWT" in content_data[i]:
                        size = content_data[i].replace("*", '').split("X")[1].split(term)[0].replace(" ", '').replace("'", '&#39;')
                        for key, value in size_data.items():
                            if key == size:
                                size = value

                        if "AS IS" in content_data[i]:
                            term = "Used"
                            term_temp = "AS IS "
                        if "WWT" in content_data[i]:
                            term = "WWT"
                            term_temp = "WWT "

                        feature = content_data[i].split(term_temp)[1].split(" - ")[0].replace(" ", '')
                        price = content_data[i].replace("*", "").split(" - ")[1].replace("$", "").replace(",", "").replace("EACH", "").strip()
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for jenny@icc-solution.com (Jenny Roberts, International Container & Chassis Solution) --------------- #
        case "jenny@icc-solution.com":
            clear_container_data(vendor_email[0])
            provider = "Jenny Roberts, International Container & Chassis Solution"
            content_data = content.split("Regards,")[0].split("\n")
            for item in content_data:
                item = item.strip()
                if "*" in item and "," in item:
                    location = item.replace("*", "").split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value
                else:
                    feature, depot, eta = "", "", ""
                    item = item.replace("X", "x")
                    if "x" in item:
                        quantity = item.split("x")[0]
                        quantity = int(quantity) if quantity.isdigit() else 1
                        size = item.split("x")[1].lstrip().split(" ")[0].replace("'", "").replace(" ", "")
                    else:
                        quantity = 1
                        size = item.split(" ")[0].replace("'", "").replace(" ", "")

                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    if "CW" in item or "WWT" in item or "IICL" in item:
                        terms = ["CW", "WWT", "IICL"]
                        term = next((t for t in terms if t in item), None)
                        price = item.split("$")[1].replace(",", "").replace("each", "").strip()
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                    if "New/ One Trip" in item:
                        term = "1Trip"
                        if "(" in item and ")" in item:
                            feature = item.split("(")[1].split(")")[0]
                        price = item.split("$")[1].replace(",", "").replace("each", "").strip()
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for erica@icc-solution.com (Erica Medina, International Container & Chassis Solution) --------------- #
        case "erica@icc-solution.com":
            clear_container_data(vendor_email[0])
            provider = "Erica Medina, International Container & Chassis Solution"
            content_data = content.split("Regards,")[0].split("\n")
            for item in content_data:
                item = item.strip()
                if "*" in item and "," in item:
                    location = item.replace("*", "").split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value
                else:
                    feature, depot, eta = "", "", ""
                    item = item.replace("X", "x")
                    if "x" in item:
                        quantity = item.split("x")[0]
                        quantity = int(quantity) if quantity.isdigit() else 1
                        size = item.split("x")[1].lstrip().split(" ")[0].replace("'", "").replace(" ", "")
                    else:
                        quantity = 1
                        size = item.split(" ")[0].replace("'", "").replace(" ", "")

                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    if "CW" in item or "WWT" in item or "IICL" in item:
                        terms = ["CW", "WWT", "IICL"]
                        term = next((t for t in terms if t in item), None)
                        price = item.split("$")[1].replace(",", "").replace("each", "").strip()
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

                    if "New/ One Trip" in item:
                        term = "1Trip"
                        if "(" in item and ")" in item:
                            feature = item.split("(")[1].split(")")[0]
                        price = item.split("$")[1].replace(",", "").replace("each", "").strip()
                        price = int(price) if price.isdigit() else 0

                        insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

        # ---------------  Parsing for gemparker@scontainers.com (Gem Parker, Star Container Solution) --------------- #
        case "gemparker@scontainers.com":
            clear_container_data(vendor_email[0])
            provider = "Gem Parker, Star Container Solution"
            content_data = content.split("\n")

            for item in content_data:
                if item.count("*") == 2 and "[" not in item:
                    location = item.replace("*", "").split(",")[0].upper().strip()
                    for key, value in location_data.items():
                        if key == location:
                            location = value
                if item.count("*") == 2 and "$" in item and "[" in item:
                    size = item.split(" ")[1].replace(" ", "").replace("'", "")
                    for key, value in size_data.items():
                        if key == size:
                            size = value

                    depot, eta = "", ""
                    term = "1Trip"
                    feature = item.split("(")[1].split(")")[0].replace("FULL OPEN SIDE;", "").strip()
                    quantity = item.split("]")[0].replace("[", "").strip()
                    quantity = int(quantity) if quantity.isdigit() else 1
                    price = item.split("$")[1].replace(",", "").replace("EACH", "").strip()
                    price = int(price) if price.isdigit() else 0

                    insert_container_record(connection, size, quantity, term, location, price, feature, depot, eta, provider, vendor_email[0], received_date, created_date)

            return

    # Close the connection
    if connection:
        connection.close()

    return body

def parse_html_content(html_content):
    # Initialize BeautifulSoup with the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all <tr> elements
    rows = soup.find_all('tr')

    for i in range(0, len(rows)):
        cells = rows[i].find_all('td')
        cell_data = [cell.get_text() for cell in cells]
        print(cell_data)

def get_today_emails():
    # Define the path to your token file and credentials
    creds = Credentials.from_authorized_user_file('token.json', ['https://www.googleapis.com/auth/gmail.readonly'])

    # Connect to Gmail API
    service = build('gmail', 'v1', credentials=creds)

    current_datetime = datetime.now()
    yesterday = current_datetime - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y/%m/%d")
    query = f"after:{yesterday_str}"

    # Search for emails from today
    result = service.users().messages().list(userId='me', q=query).execute()
    messages = result.get('messages', [])

    email_data = []
    # Process each email
    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id']).execute()

        # Print subject and snippet
        for header in msg['payload']['headers']:
            if header['name'] == 'Subject':
                print("Subject:", header['value'])
            if header['name'] == 'From':
                vendor_email = re.findall(r'<(.*?)>', header['value'])
                email_data.append(vendor_email[0])
                print("Email:", vendor_email[0])
        print("Snippet:", msg['snippet'])

    print("\n")
    print("Email list:", email_data)

def main():
    # Authenticate and build the service
    service = authenticate_gmail()

    with open('variable.json', 'r') as f:
        var_data = json.load(f)
    email_html_lists = var_data['email_html_data']
    email_plain_lists = var_data['email_plain_data']

    current_datetime = datetime.now()
    yesterday = current_datetime - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y/%m/%d")

    for email_html_list in email_html_lists:
        try:
            query = f"from:{email_html_list} after:{yesterday_str}"
            print(query)
            messages = get_messages(service, query=query)
            if messages:
                for message in messages:
                    get_message_content_html(service, message['id'])
        except Exception as e:
            print(f"Error on item {email_html_list}: {e}")  # Handle the error and continue

    # for email_plain_list in email_plain_lists:
    #     query = f"from:{email_plain_list} after:{yesterday_str}"
    #     messages = get_messages(service, query=query)
    #     if messages:
    #         for message in messages:
    #             get_message_content_plain(service, message['id'])

    # query = "from:e4.mevtnhrict@gcc2011.com after:2025/3/15"
    # messages = get_messages(service, query=query)
    # if messages:
    #     for message in messages:
    #         get_message_content_html(service, message['id'])

if __name__ == '__main__':
    main()
