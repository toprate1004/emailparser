import os
import base64
import pymysql
import re

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

# from flask import Flask, jsonify, request
# app = Flask(__name__)

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
                quantity VARCHAR(255),
                size VARCHAR(255),
                type VARCHAR(255),
                term VARCHAR(255),
                grade VARCHAR(255),
                price VARCHAR(255),
                feature VARCHAR(255),
                full_line VARCHAR(255),
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
    except Error as e:
        print(f"The error '{e}' occurred")


def get_container_data():
    # Connect to the MySQL database
    host = "localhost"
    user = "root"
    password = os.getenv("MYSQL_PASSWORD")
    database = "container"

    # Create a connection
    conn = create_connection(host, user, password, database)
    
    try:
        with conn.cursor() as cursor:
            # SQL query to fetch data
            fetch_query = "SELECT location, quantity FROM container"
            cursor.execute(fetch_query)

            # Fetch all results
            container_data = cursor.fetchall()
            container_json_data = [{"location": row[0], "quantity": row[1]} for row in container_data]
    
    except Exception as e:
        print("Error fetching data:", e)

    # Close the connection
    if conn:
        conn.close()

    return container_json_data


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

    # Replace the following variables with your database credentials
    # Connect to the MySQL database
    host = "localhost"
    user = "root"
    password = os.getenv("MYSQL_PASSWORD")
    database = "container"

    # Create a connection
    conn = create_connection(host, user, password, database)

    # Get the email body
    if email_message.is_multipart():
        for part in email_message.walk():
            if part.get_content_type() == 'text/html':
                body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8')
                # print("\nBody:", body)
    else:
        body = email_message.get_payload(decode=True).decode(email_message.get_content_charset() or 'utf-8')
        # print("\nBody:", body)

    vendor_email = re.findall(r'<(.*?)>', email_message['From'])
    received_date = email_message['Date']
    subject = email_message['Subject']
    current_datetime = datetime.now()
    created_date = current_datetime.strftime("%Y/%m/%d %H:%M:%S")

    soup = BeautifulSoup(body, 'html.parser')

    print(subject)
    print(vendor_email[0])
    # parse_html_content(body)

    # Find all <tr> elements
    rows = soup.find_all('tr')

    match vendor_email[0]:
        # ---------------  Parsing for john@americanacontainers.com (John Rupert, Americana Containers Distribution Chain) --------------- #
        case "john@americanacontainers.com":
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                location = cell_data[0]
                grade = cell_data[1].replace(" ", '').replace("20'", '').replace("40'", '').replace("HC", '').replace("STD", '')
                size = cell_data[1].replace(" ", '').replace(grade, '').replace("'", '&#39;')
                term = cell_data[2]
                quantity = cell_data[3]
                price = re.sub(r'[^\d]', '', cell_data[4])

                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'John Rupert, Americana Containers Distribution Chain', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                execute_query(conn, insert)
            
            return

        # ---------------  Parsing for tine@americanacontainers.com (Tine Patterson, Americana Containers Distribution Chain) --------------- #
        case "tine@americanacontainers.com":
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) == 5:
                    location = cell_data[0]
                    grade = cell_data[1].replace(" ", '').replace("20'", '').replace("40'", '').replace("HC", '').replace("STD", '')
                    size = cell_data[1].replace(" ", '').replace(grade, '').replace("'", '&#39;')
                    term = cell_data[2]
                    quantity = cell_data[3]
                    price = re.sub(r'[^\d]', '', cell_data[4])

                    insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Tine Patterson, Americana Containers Distribution Chain', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                    execute_query(conn, insert)
            
            return
            
        # ---------------  Parsing for johannes@oztradingltd.com (Johannes, OZ Trading Limited) --------------- #
        case "johannes@oztradingltd.com":
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                size = cell_data[1]
                quantity = cell_data[2]
                location = cell_data[3]
                eta = cell_data[4]
                price = cell_data[6]
                if "NEW" in cell_data[5]:
                    term = "NEW"
                    feature = cell_data[5].replace("NEW ", '')
                else:
                    term = "CW"
                    feature = ""

                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '', '{price}', '{feature}', '', '{eta}', '', 'Johannes, OZ Trading Limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                execute_query(conn, insert)
            
            return
        
        # ---------------  Parsing for steven.gao@cgkinternational.com (Steven Gao, CGK International Limited) --------------- #
        case "steven.gao@cgkinternational.com":    
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                size = cell_data[0].replace("'", '&#39;')
                location = cell_data[1]
                term = cell_data[5]
                quantity = cell_data[2]
                price = cell_data[3]
                feature = cell_data[6] + " YOM:" + cell_data[4]
                
                if quantity.isdigit():
                    insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '', '{price}', '{feature}', '', '', '', 'Steven Gao, CGK International Limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                    execute_query(conn, insert)
                else:
                    i += 1
            
            return

        # ---------------  Parsing for sales@isr-containers.com (Zarah M) --------------- #
        case "sales@isr-containers.com":    
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                state = cell_data[0]
                city = cell_data[1]
                location = city + ", " + state
                size = cell_data[2].replace("'", '&#39;')
                term = cell_data[3]
                grade = cell_data[4].replace("'", '&#39;')
                quantity = cell_data[5]
                
                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '', '', '', '', '', 'Zarah M', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                execute_query(conn, insert)
            
            return

        # ---------------  Parsing for wayne.vandenburg@dutchcontainers.com (Wayne van den Burg, Dutch Container Merchants B.V.) --------------- #
        case "wayne.vandenburg@dutchcontainers.com":
            if "Arrival" in subject:
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    location = cell_data[0].replace('\n', '')
                    size = cell_data[2].replace('\n', '').replace("'", '&#39;')
                    type = cell_data[3].replace('\n', '')
                    quantity = cell_data[1].replace('\n', '')
                    feature = cell_data[4].replace('\n', '')
                    eta = cell_data[5].replace('\n', '')

                    insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '{type}', '', '{location}', '', '', '{feature}', '', '{eta}', '', 'Wayne van den Burg, Dutch Container Merchants B.V.', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                    execute_query(conn, insert)

            elif "Inventory" in subject: 
                country = ''
                for i in range(1, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    if len([item for item in cell_data if item == "\n\xa0\n"]) >= 5:
                        country = cell_data[0].replace('\n', '')
                        i += 1
                    else:
                        location = cell_data[0].replace('\n', '') + ", " + country
                        quantity = cell_data[1].replace('\n', '')
                        size = cell_data[2].replace('\n', '').replace("'", '&#39;')
                        type = cell_data[3].replace('\n', '').replace("'", '&#39;')
                        price = cell_data[6].replace('\n', '').split(',')[0]
                        depot = cell_data[5].replace('\n', '')
                        feature = cell_data[4].replace('\n', '')

                        if quantity.isdigit():
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '{type}', '', '{location}', '', '{price}', '{feature}', '{depot}', '', '', 'Wayne van den Burg, Dutch Container Merchants B.V.', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)
                        else:
                            i += 1
            
            return
        
        # ---------------  Parsing for  wayne.vandenburg@trident-containers.com (Wayne van den Burg, Trident Container Leasing B.V.) --------------- #
        case "wayne.vandenburg@trident-containers.com":
            status = ''
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if "ARRIVING" in cell_data[0]:
                    status = "ARRIVING"
                location = cell_data[0].replace('\n', '')
                quantity = cell_data[1].replace('\n', '')
                size = cell_data[2].replace('\n', '').replace("'", '&#39;')
                type = cell_data[3].replace('\n', '').replace("'", '&#39;')
                price = cell_data[6].replace('\n', '').split(',')[0]
                feature = cell_data[4].replace('\n', '')
                depot = cell_data[5].replace('\n', '')
                
                if status:
                    if quantity.isdigit():
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '{type}', '', '{location}', '', '{price}', '{feature}', '', '{depot}', '', 'Wayne van den Burg, Trident Container Leasing B.V.', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)
                    else:
                        i += 1
                else:
                    if quantity.isdigit():
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '{type}', '', '{location}', '', '{price}', '{feature}', '{depot}', '', '', 'Wayne van den Burg, Trident Container Leasing B.V.', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)
                    else:
                        i += 1
            
            return

        # ---------------  Parsing for  ryan@trident-containers.com (Ryan Garrido, Trident Container Leasing B.V.) --------------- #
        case "ryan@trident-containers.com":
            country = ''
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len([item for item in cell_data if item == "\n\xa0\n"]) >= 5:
                    country = cell_data[0].replace('\n', '')
                    i += 1
                else:
                    location = cell_data[0].replace('\n', '') + ", " + country
                    quantity = cell_data[1].replace('\n', '')
                    size = cell_data[2].replace('\n', '').replace("'", '&#39;')
                    type = cell_data[3].replace('\n', '').replace("'", '&#39;')
                    price = cell_data[6].replace('\n', '').split(',')[0]
                    depot = cell_data[5].replace('\n', '')
                    feature = cell_data[4].replace('\n', '')
                    
                    if quantity.isdigit():
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '{type}', '', '{location}', '', '{price}', '{feature}', '{depot}', '', '', 'Ryan Garrido, Trident Container Leasing B.V.', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)
                    else:
                        i += 1
            
            return
        
        # ---------------  Parsing for  e4.mevtnhrict@gcc2011.com (Oliver Egonio, Global Container & Chassis) --------------- #
        case "e4.mevtnhrict@gcc2011.com":
            if "Inventory" in subject:
                sizes = rows[0].find_all('td')
                size_data = [size.get_text() for size in sizes]
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    state = cell_data[0]
                    city = cell_data[1]
                    location = city + ", " + state
                    grade = cell_data[2].replace("'", '&#39;')
                    term = cell_data[3]

                    if cell_data[4] and cell_data[5]:
                        size = size_data[4].replace("'", '&#39;')
                        quantity = cell_data[4]
                        price = re.sub(r'[^\d]', '', cell_data[5])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Oliver Egonio, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[6] and cell_data[7]:
                        size = size_data[5].replace("'", '&#39;')
                        quantity = cell_data[6]
                        price = re.sub(r'[^\d]', '', cell_data[7])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Oliver Egonio, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[8] and cell_data[9]:
                        size = size_data[6].replace("'", '&#39;')
                        quantity = cell_data[8]
                        price = re.sub(r'[^\d]', '', cell_data[9])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Oliver Egonio, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[10] and cell_data[11]:
                        size = size_data[7].replace("'", '&#39;')
                        quantity = cell_data[10]
                        price = re.sub(r'[^\d]', '', cell_data[11])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Oliver Egonio, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if len(cell_data) > 12:
                        if cell_data[12] and cell_data[13]:
                            size = size_data[8].replace("'", '&#39;')
                            quantity = cell_data[12]
                            price = re.sub(r'[^\d]', '', cell_data[13])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Oliver Egonio, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 14:
                        if cell_data[14] and cell_data[15]:
                            size = size_data[9].replace("'", '&#39;')
                            quantity = cell_data[14]
                            price = re.sub(r'[^\d]', '', cell_data[15])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Oliver Egonio, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 16:
                        if cell_data[16] and cell_data[17]:
                            size = size_data[10].replace("'", '&#39;')
                            quantity = cell_data[16]
                            price = re.sub(r'[^\d]', '', cell_data[17])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Oliver Egonio, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)          

            return
        
        # ---------------  Parsing for  e8.pa@gcc2011.com (Gerone Rustia, Global Container & Chassis) --------------- #
        case "e8.pa@gcc2011.com":
            if "Inventory" in subject:
                sizes = rows[0].find_all('td')
                size_data = [size.get_text() for size in sizes]
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    state = cell_data[0]
                    city = cell_data[1]
                    location = city + ", " + state
                    grade = cell_data[2].replace("'", '&#39;')
                    term = cell_data[3]

                    if cell_data[4] and cell_data[5]:
                        size = size_data[4].replace("'", '&#39;')
                        quantity = cell_data[4]
                        price = re.sub(r'[^\d]', '', cell_data[5])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Gerone Rustia, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[6] and cell_data[7]:
                        size = size_data[5].replace("'", '&#39;')
                        quantity = cell_data[6]
                        price = re.sub(r'[^\d]', '', cell_data[7])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Gerone Rustia, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[8] and cell_data[9]:
                        size = size_data[6].replace("'", '&#39;')
                        quantity = cell_data[8]
                        price = re.sub(r'[^\d]', '', cell_data[9])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Gerone Rustia, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[10] and cell_data[11]:
                        size = size_data[7].replace("'", '&#39;')
                        quantity = cell_data[10]
                        price = re.sub(r'[^\d]', '', cell_data[11])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Gerone Rustia, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)
                        
                    if len(cell_data) > 12:
                        if cell_data[12] and cell_data[13]:
                            size = size_data[8].replace("'", '&#39;')
                            quantity = cell_data[12]
                            price = re.sub(r'[^\d]', '', cell_data[13])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Gerone Rustia, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 14:
                        if cell_data[14] and cell_data[15]:
                            size = size_data[9].replace("'", '&#39;')
                            quantity = cell_data[14]
                            price = re.sub(r'[^\d]', '', cell_data[15])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Gerone Rustia, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 16:
                        if cell_data[16] and cell_data[17]:
                            size = size_data[10].replace("'", '&#39;')
                            quantity = cell_data[16]
                            price = re.sub(r'[^\d]', '', cell_data[17])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Gerone Rustia, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

            return

        # ---------------  Parsing for e61.md@gcc2011.com (Jeni Bobias, Global Container & Chassis) --------------- #
        case "e61.md@gcc2011.com":    
            if "Inventory" in subject:
                sizes = rows[0].find_all('td')
                size_data = [size.get_text() for size in sizes]
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    state = cell_data[0]
                    city = cell_data[1]
                    location = city + ", " + state
                    grade = cell_data[2].replace("'", '&#39;')
                    term = cell_data[3]

                    if cell_data[4] and cell_data[5]:
                        size = size_data[4].replace("'", '&#39;')
                        quantity = cell_data[4]
                        price = re.sub(r'[^\d]', '', cell_data[5])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeni Bobias, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[6] and cell_data[7]:
                        size = size_data[5].replace("'", '&#39;')
                        quantity = cell_data[6]
                        price = re.sub(r'[^\d]', '', cell_data[7])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeni Bobias, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[8] and cell_data[9]:
                        size = size_data[6].replace("'", '&#39;')
                        quantity = cell_data[8]
                        price = re.sub(r'[^\d]', '', cell_data[9])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeni Bobias, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[10] and cell_data[11]:
                        size = size_data[7].replace("'", '&#39;')
                        quantity = cell_data[10]
                        price = re.sub(r'[^\d]', '', cell_data[11])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeni Bobias, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if len(cell_data) > 12:
                        if cell_data[12] and cell_data[13]:
                            size = size_data[8].replace("'", '&#39;')
                            quantity = cell_data[12]
                            price = re.sub(r'[^\d]', '', cell_data[13])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeni Bobias, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 14:
                        if cell_data[14] and cell_data[15]:
                            size = size_data[9].replace("'", '&#39;')
                            quantity = cell_data[14]
                            price = re.sub(r'[^\d]', '', cell_data[15])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeni Bobias, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 16:
                        if cell_data[16] and cell_data[17]:
                            size = size_data[10].replace("'", '&#39;')
                            quantity = cell_data[16]
                            price = re.sub(r'[^\d]', '', cell_data[17])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeni Bobias, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)
            
            return
        
        # ---------------  Parsing for  W3.Wa@gcc2011.com (Eddie Pamplona Jr, Global Container & Chassis) --------------- #
        case "W3.Wa@gcc2011.com":
            if "inventory" in subject:
                sizes = rows[0].find_all('td')
                size_data = [size.get_text() for size in sizes]
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    state = cell_data[0]
                    city = cell_data[1]
                    location = city + ", " + state
                    grade = cell_data[2].replace("'", '&#39;')
                    term = cell_data[3]

                    if cell_data[4] and cell_data[5]:
                        size = size_data[4].replace("'", '&#39;')
                        quantity = cell_data[4]
                        price = re.sub(r'[^\d]', '', cell_data[5])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Eddie Pamplona Jr, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[6] and cell_data[7]:
                        size = size_data[5].replace("'", '&#39;')
                        quantity = cell_data[6]
                        price = re.sub(r'[^\d]', '', cell_data[7])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Eddie Pamplona Jr, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[8] and cell_data[9]:
                        size = size_data[6].replace("'", '&#39;')
                        quantity = cell_data[8]
                        price = re.sub(r'[^\d]', '', cell_data[9])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Eddie Pamplona Jr, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[10] and cell_data[11]:
                        size = size_data[7].replace("'", '&#39;')
                        quantity = cell_data[10]
                        price = re.sub(r'[^\d]', '', cell_data[11])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Eddie Pamplona Jr, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if len(cell_data) > 12:
                        if cell_data[12] and cell_data[13]:
                            size = size_data[8].replace("'", '&#39;')
                            quantity = cell_data[12]
                            price = re.sub(r'[^\d]', '', cell_data[13])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Eddie Pamplona Jr, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 14:
                        if cell_data[14] and cell_data[15]:
                            size = size_data[9].replace("'", '&#39;')
                            quantity = cell_data[14]
                            price = re.sub(r'[^\d]', '', cell_data[15])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Eddie Pamplona Jr, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 16:
                        if cell_data[16] and cell_data[17]:
                            size = size_data[10].replace("'", '&#39;')
                            quantity = cell_data[16]
                            price = re.sub(r'[^\d]', '', cell_data[17])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Eddie Pamplona Jr, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 18:
                        if cell_data[18] and cell_data[19]:
                            size = size_data[11].replace("'", '&#39;')
                            quantity = cell_data[18]
                            price = re.sub(r'[^\d]', '', cell_data[19])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Eddie Pamplona Jr, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

            return
        
        # ---------------  Parsing for  W6.CaLgb@gcc2011.com (Caryn Saringo, Global Container & Chassis) --------------- #
        case "W6.CaLgb@gcc2011.com":
            if "Containers" in subject:
                sizes = rows[0].find_all('td')
                size_data = [size.get_text() for size in sizes]
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    state = cell_data[0]
                    city = cell_data[1]
                    location = city + ", " + state
                    grade = cell_data[2].replace("'", '&#39;')
                    term = cell_data[3]

                    if cell_data[4] and cell_data[5]:
                        size = size_data[4].replace("'", '&#39;')
                        quantity = cell_data[4]
                        price = re.sub(r'[^\d]', '', cell_data[5])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Caryn Saringo, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[6] and cell_data[7]:
                        size = size_data[5].replace("'", '&#39;')
                        quantity = cell_data[6]
                        price = re.sub(r'[^\d]', '', cell_data[7])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Caryn Saringo, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[8] and cell_data[9]:
                        size = size_data[6].replace("'", '&#39;')
                        quantity = cell_data[8]
                        price = re.sub(r'[^\d]', '', cell_data[9])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Caryn Saringo, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[10] and cell_data[11]:
                        size = size_data[7].replace("'", '&#39;')
                        quantity = cell_data[10]
                        price = re.sub(r'[^\d]', '', cell_data[11])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Caryn Saringo, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if len(cell_data) > 12:
                        if cell_data[12] and cell_data[13]:
                            size = size_data[8].replace("'", '&#39;')
                            quantity = cell_data[12]
                            price = re.sub(r'[^\d]', '', cell_data[13])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Caryn Saringo, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 14:
                        if cell_data[14] and cell_data[15]:
                            size = size_data[9].replace("'", '&#39;')
                            quantity = cell_data[14]
                            price = re.sub(r'[^\d]', '', cell_data[15])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Caryn Saringo, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 16:
                        if cell_data[16] and cell_data[17]:
                            size = size_data[10].replace("'", '&#39;')
                            quantity = cell_data[16]
                            price = re.sub(r'[^\d]', '', cell_data[17])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Caryn Saringo, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)          

            return
                
        # ---------------  Parsing for  W8.CaLgb@gcc2011.com (Jayvie Hernaez, Global Container & Chassis) --------------- #
        case "W8.CaLgb@gcc2011.com":
            if "Inventory" in subject:
                sizes = rows[0].find_all('td')
                size_data = [size.get_text() for size in sizes]
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    state = cell_data[0]
                    city = cell_data[1]
                    location = city + ", " + state
                    grade = cell_data[2].replace("'", '&#39;')
                    term = cell_data[3]

                    if cell_data[4] and cell_data[5]:
                        size = size_data[4].replace("'", '&#39;')
                        quantity = cell_data[4]
                        price = re.sub(r'[^\d]', '', cell_data[5])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jayvie Hernaez, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[6] and cell_data[7]:
                        size = size_data[5].replace("'", '&#39;')
                        quantity = cell_data[6]
                        price = re.sub(r'[^\d]', '', cell_data[7])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jayvie Hernaez, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[8] and cell_data[9]:
                        size = size_data[6].replace("'", '&#39;')
                        quantity = cell_data[8]
                        price = re.sub(r'[^\d]', '', cell_data[9])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jayvie Hernaez, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[10] and cell_data[11]:
                        size = size_data[7].replace("'", '&#39;')
                        quantity = cell_data[10]
                        price = re.sub(r'[^\d]', '', cell_data[11])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jayvie Hernaez, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if len(cell_data) > 12:
                        if cell_data[12] and cell_data[13]:
                            size = size_data[8].replace("'", '&#39;')
                            quantity = cell_data[12]
                            price = re.sub(r'[^\d]', '', cell_data[13])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jayvie Hernaez, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)      

            return

        # ---------------  Parsing for  c6.wi@gcc2011.com (Jeohnel Erfe, Global Container & Chassis) --------------- #
        case "c6.wi@gcc2011.com":
            if "Containers" in subject:
                sizes = rows[0].find_all('td')
                size_data = [size.get_text() for size in sizes]
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    state = cell_data[0]
                    city = cell_data[1]
                    location = city + ", " + state
                    grade = cell_data[2].replace("'", '&#39;')
                    term = cell_data[3]

                    if cell_data[4] and cell_data[5]:
                        size = size_data[4].replace("'", '&#39;')
                        quantity = cell_data[4]
                        price = re.sub(r'[^\d]', '', cell_data[5])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeohnel Erfe, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[6] and cell_data[7]:
                        size = size_data[5].replace("'", '&#39;')
                        quantity = cell_data[6]
                        price = re.sub(r'[^\d]', '', cell_data[7])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeohnel Erfe, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[8] and cell_data[9]:
                        size = size_data[6].replace("'", '&#39;')
                        quantity = cell_data[8]
                        price = re.sub(r'[^\d]', '', cell_data[9])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeohnel Erfe, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[10] and cell_data[11]:
                        size = size_data[7].replace("'", '&#39;')
                        quantity = cell_data[10]
                        price = re.sub(r'[^\d]', '', cell_data[11])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeohnel Erfe, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if len(cell_data) > 12:
                        if cell_data[12] and cell_data[13]:
                            size = size_data[8].replace("'", '&#39;')
                            quantity = cell_data[12]
                            price = re.sub(r'[^\d]', '', cell_data[13])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeohnel Erfe, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 14:
                        if cell_data[14] and cell_data[15]:
                            size = size_data[9].replace("'", '&#39;')
                            quantity = cell_data[14]
                            price = re.sub(r'[^\d]', '', cell_data[15])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Jeohnel Erfe, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)        

            return
        
        # ---------------  Parsing for  c17.txelp@gcc2011.com (Raffy Santos, Global Container & Chassis) --------------- #
        case "c17.txelp@gcc2011.com":
            sizes = rows[0].find_all('td')
            size_data = [size.get_text() for size in sizes]
            if "containers" in subject:
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    state = cell_data[0]
                    city = cell_data[1]
                    location = city + ", " + state
                    grade = cell_data[2].replace("'", '&#39;')
                    term = cell_data[3]

                    if cell_data[4] and cell_data[5]:
                        size = size_data[4].replace("'", '&#39;')
                        quantity = cell_data[4]
                        price = re.sub(r'[^\d]', '', cell_data[5])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Raffy Santos, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[6] and cell_data[7]:
                        size = size_data[5].replace("'", '&#39;')
                        quantity = cell_data[6]
                        price = re.sub(r'[^\d]', '', cell_data[7])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Raffy Santos, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[8] and cell_data[9]:
                        size = size_data[6].replace("'", '&#39;')
                        quantity = cell_data[8]
                        price = re.sub(r'[^\d]', '', cell_data[9])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Raffy Santos, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if len(cell_data) > 10:
                        if cell_data[10] and cell_data[11]:
                            size = size_data[7].replace("'", '&#39;')
                            quantity = cell_data[10]
                            price = re.sub(r'[^\d]', '', cell_data[11])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Raffy Santos, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 12:
                        if cell_data[12] and cell_data[13]:
                            size = size_data[8].replace("'", '&#39;')
                            quantity = cell_data[12]
                            price = re.sub(r'[^\d]', '', cell_data[13])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Raffy Santos, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert) 

            return
        
        # ---------------  Parsing for  m1.ntab@gcc2011.com (Rey Dawana, Global Container & Chassis) --------------- #
        case "m1.ntab@gcc2011.com":
            if "Container" in subject:
                sizes = rows[0].find_all('td')
                size_data = [size.get_text() for size in sizes]
                for i in range(2, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]

                    state = cell_data[0]
                    city = cell_data[1]
                    location = city + ", " + state
                    grade = cell_data[2].replace("'", '&#39;')
                    term = cell_data[3]

                    if cell_data[4] and cell_data[5]:
                        size = size_data[4].replace("'", '&#39;')
                        quantity = cell_data[4]
                        price = re.sub(r'[^\d]', '', cell_data[5])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Rey Dawana, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[6] and cell_data[7]:
                        size = size_data[5].replace("'", '&#39;')
                        quantity = cell_data[6]
                        price = re.sub(r'[^\d]', '', cell_data[7])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Rey Dawana, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[8] and cell_data[9]:
                        size = size_data[6].replace("'", '&#39;')
                        quantity = cell_data[8]
                        price = re.sub(r'[^\d]', '', cell_data[9])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Rey Dawana, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if cell_data[10] and cell_data[11]:
                        size = size_data[7].replace("'", '&#39;')
                        quantity = cell_data[10]
                        price = re.sub(r'[^\d]', '', cell_data[11])
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Rey Dawana, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if len(cell_data) > 12:
                        if cell_data[12] and cell_data[13]:
                            size = size_data[8].replace("'", '&#39;')
                            quantity = cell_data[12]
                            price = re.sub(r'[^\d]', '', cell_data[13])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Rey Dawana, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                    if len(cell_data) > 14:
                        if cell_data[14] and cell_data[15]:
                            size = size_data[9].replace("'", '&#39;')
                            quantity = cell_data[14]
                            price = re.sub(r'[^\d]', '', cell_data[15])
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Rey Dawana, Global Container & Chassis', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)       

            return

        # ---------------  Parsing for ash@container-xchange.com (Ashish Sharma, XChange) --------------- #
        case "ash@container-xchange.com":
            size_data = ['20&#39;', '20&#39;', '20&#39;', '20&#39;', '40&#39; HC', '40&#39; HC', '40&#39; HC', '40&#39; HC']
            grade_data = ['', '', 'Double Door', 'Side Door', '', '', 'Double Door', 'Side Door']
            term_data = ['Cargo Worthy', '1 Trip', '1 Trip', '1 Trip', 'Cargo Worthy', '1 Trip', '1 Trip', '1 Trip']
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]
                if "\n\xa0\n" in cell_data[0]:
                    i += 1
                else:
                    location = cell_data[0].replace('\n', '')
                    for j in range(1, len(cell_data)):
                        size = size_data[j-1]
                        grade = grade_data[j-1]
                        term = term_data[j-1]
                        price = cell_data[j].replace('\n', '').replace('$', '').replace(' ', '')
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '', '', '{term}', '{location}', '{grade}', '{price}', '', '', '', '', 'Ashish Sharma, XChange', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)
            
            return
        
        # ---------------  Parsing for Saquib.amiri@sadecontainers.com (Saquib Amiri, SADE Containers GmbH) --------------- #
        case "Saquib.amiri@sadecontainers.com":
            if "Inventory" in subject:
                for i in range(1, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]
                    
                    location = cell_data[0].replace('\n', '')
                    size = cell_data[1].replace('\n', '').split(' ')[0].replace("'", '&#39;')
                    size_temp = cell_data[1].replace('\n', '').split(' ')[0] + " "
                    term = cell_data[1].replace('\n', '').replace(size_temp, '')
                    depot = cell_data[2].replace('\n', '')
                    quantity = cell_data[3].replace('\n', '')
                    price = cell_data[4].replace('\n', '').replace('$', '')
                    insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '', '', '{term}', '{location}', '', '{price}', '', '{depot}', '', '', 'Saquib Amiri, SADE Containers GmbH', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                    execute_query(conn, insert)
            
            return

        # ---------------  Parsing for Saquib.amiri@sadecontainers.com (Jack Anguish, ISM) --------------- #
        case "JAnguish@ism247.com":
            if "Inventory" in subject:
                location = ''
                status = ''
                for i in range(1, len(rows)):
                    cells = rows[i].find_all('td')
                    cell_data = [cell.get_text() for cell in cells]    
                    if len(cell_data) == 0 or len(cell_data) == 1 or len(cell_data) == 4 or len(cell_data) == 7:
                        i += 1
                        
                    if len(cell_data) == 2 and "Available" in cell_data[1]:
                        if "Location" in cell_data[0]:
                            i += 1
                        else:
                            location = cell_data[0].replace('\n', '')
                    elif len(cell_data) == 5:
                        location = cell_data[0].replace('\n', '')
                        status = cell_data[4].replace('\n', '').replace('\xa0', '')

                    if len(cell_data) == 6 and status == "Price":
                        quantity = cell_data[0].replace('\n', '').replace('\r', '')
                        grade = cell_data[1].replace('\n', '').replace('\r', '').replace('\xa0', '').replace(" ", '').replace("20'", '').replace("40'", '').replace("53'", '').replace("HC", '').replace("STD", '')
                        size = cell_data[1].replace('\n', '').replace('\r', '').replace('\xa0', '').replace(" ", '').replace(grade, '').replace("'", '&#39;')
                        term = cell_data[2].replace('\n', '').replace('\r', '').replace('\xa0', '')
                        feature = cell_data[3].replace('\n', '').replace('\r', '').replace('\xa0', '') + ", " + cell_data[4].replace('\n', '').replace('\r', '').replace('\xa0', '')
                        price = cell_data[5].replace('\n', '').replace('\r', '').replace('\xa0', '').replace('$', '').replace(',', '')
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '', 'Jack Anguish, ISM', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    if len(cell_data) == 6 and status == "ETA":
                        quantity = cell_data[0].replace('\n', '').replace('\r', '')
                        grade = cell_data[1].replace('\n', '').replace('\r', '').replace('\xa0', '').replace(" ", '').replace("20'", '').replace("40'", '').replace("53'", '').replace("HC", '').replace("STD", '')
                        size = cell_data[1].replace('\n', '').replace('\r', '').replace('\xa0', '').replace(" ", '').replace(grade, '').replace("'", '&#39;')
                        term = cell_data[2].replace('\n', '').replace('\r', '').replace('\xa0', '')
                        feature = cell_data[3].replace('\n', '').replace('\r', '').replace('\xa0', '') + ", " + cell_data[4].replace('\n', '').replace('\r', '').replace('\xa0', '')
                        eta = cell_data[5].replace('\n', '').replace('\r', '').replace('\xa0', '')
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '', '', '{term}', '{location}', '{grade}', '', '{feature}', '', '{eta}', '', 'Jack Anguish, ISM', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)
            
            return

        # ---------------  Parsing for sales@tritoncontainersales.com (TRITON) --------------- #
        case "sales@tritoncontainersales.com":
            term_reefer = "CW Insulated Box / Non-Working Reefer"
            term_tripped = "CW Working Condition & Pre-Tripped"
            for i in range(6, len(rows)-2):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                location = cell_data[0].replace('\n', '').replace('\r', '')
                price_reefer = cell_data[1].replace('$', '').replace(',', '')
                price_tripped = cell_data[2].replace('$', '').replace(',', '')
                
                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('', '', '', '{term_reefer}', '{location}', '', '{price_reefer}', '', '', '', '', 'TRITON', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                execute_query(conn, insert)

                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('', '', '', '{term_tripped}', '{location}', '', '{price_tripped}', '', '', '', '', 'TRITON', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                execute_query(conn, insert)
            
            return
    
        # ---------------  Parsing for thomas@fulidacontainer.com (Thomas, Fulida Container Limited) --------------- #
        case "thomas@fulidacontainer.com":
            country = ''
            for i in range(0, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if cell_data[0] == "LOCATION" or cell_data[0] == "":
                    i += 1
                elif cell_data[1] == "\u3000":
                    country = cell_data[0]
                    i += 1
                else:
                    location = cell_data[0] + ", " + country
                    term = cell_data[2]
                    feature = cell_data[3] + " " + cell_data[4]
                    type = cell_data[5].replace('\xa0', '')
                    quantity = cell_data[6]
                    price = cell_data[7].replace('$', '').replace(',', '')
                    eta = cell_data[8]
                    if "（" in cell_data[1]:
                        size = cell_data[1].split("（")[0]
                        grade = cell_data[1].split("（")[1].replace('）', '')
                    elif "(" in cell_data[1]:
                        size = cell_data[1].split("(")[0]
                        grade = cell_data[1].split("(")[1].replace(')', '')
                    else:
                        size = cell_data[1]
                        grade = ''
                    insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '{type}', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '{eta}', '', 'Thomas, Fulida Container Limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                    execute_query(conn, insert)
            
            return

        # ---------------  Parsing for magui.cheung@northatlanticcontainer.com (ThomaMagui Cheungs, Account Management Associate) --------------- #
        case "magui.cheung@northatlanticcontainer.com":
            location = ''
            for i in range(1, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                size_datas = [
                    ("20' STD", "20&#39; STD"),
                    ("40' STD", "40&#39; STD"),
                    ("20' HC", "20&#39; HC"),
                    ("40' HC", "40&#39; HC"),
                    ("45' HC", "45&#39; HC"),
                    ("53' HC", "53&#39; HC")
                ]

                term_datas = ["ASIS", "CW", "WWT", "ONE TRIPPER", "IICL", "NEW", "IICL-NEW", "WORKING REFEER", "NON-WORKING REEFER", "ONE TRIPPER REEFER"]
                grade_datas = ["DOUBLE DOOR", "OPEN SIDE", "OPEN SIDE (4DOORS)", "OPEN SIDE (2DOORS)", "OPEN SIDE (FULL)"]
                type_datas = ["ARRIVING", "GATEBUY", "FOR PICK UP ASAP", "TERMINAL"]

                if len(cell_data) > 3:
                    location = cell_data[0]
                    quantity = cell_data[2]
                    price = cell_data[3].replace('$', '').replace(',', '')
                    item = cell_data[1]
                
                elif len(cell_data) > 2:
                    quantity = cell_data[1]
                    price = cell_data[2].replace('$', '').replace(',', '')
                    item = cell_data[0]

                size = ""
                for size_data, size_value in size_datas:
                    if size_data in item:
                        size = size_value
                        break  # Stop after finding the first match

                term = ""
                for term_data in term_datas:
                    if term_data in item:
                        term = term_data
                        break  # Stop after finding the first match

                grade = ""
                for grade_data in grade_datas:
                    if grade_data in item:
                        grade = grade_data
                        break  # Stop after finding the first match
                
                type = ""
                for type_data in type_datas:
                    if type_data in item:
                        type = type_data
                        break  # Stop after finding the first match

                feature = ""

                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '{type}', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '', 'ThomaMagui Cheungs, Account Management Associate', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                execute_query(conn, insert)
                
            return
    
        # ---------------  Parsing for jeff@lummid.com (Jeff Young, Lummid Containers) --------------- #
        case "jeff@lummid.com":
            location = ""
            for row in rows:
                cells = row.find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                # Check for specific cases early to set the location
                if len(cell_data) == 4 and "Market" in cell_data[0]:
                    location = "USA"

                # Continue if basic criteria aren't met
                if location and cell_data.count("\xa0") < 4 and len(cell_data) > 3 and "@" in cell_data[3]:

                    # Set location from cell_data if criteria met
                    if "\xa0" not in cell_data[0]:
                        location = cell_data[0]

                    # Extract size and term information
                    size = cell_data[1].split(" ")[0]
                    term = cell_data[1].replace(size, '').strip()

                    # Format size based on term content
                    if "ST" in term:
                        size += " ST"
                        term = term.replace("ST", '').strip()
                    
                    # Determine grade
                    grade_data = {"D.D.": "DD", "S.D.": "SD", "O.S.": "OS"}
                    grade = ""
                    for key, value in grade_data.items():
                        if key in term:
                            grade = value
                            term = term.replace(key, '').strip()

                    # Clean up `size` and format quantity, price
                    size = size.replace("'", "&#39;")
                    quantity, price = cell_data[2], ''
                    if "$" in quantity:
                        quantity, price = quantity.split(" x ")[0], cell_data[2].split("$")[1].replace(',', '')
                    
                    # Extract feature and depot information
                    feature, depot = cell_data[3].split("@")[0], cell_data[3].split("@")[1]

                    # Build and execute SQL insert statement
                    insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '', '', 'Jeff Young, Lummid Containers', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                    execute_query(conn, insert)
            
            return

        # ---------------  Parsing for eastcoast@lummid.com (Jeff Young, Lummid Containers) --------------- #
        case "eastcoast@lummid.com":
            location = ""
            for row in rows:
                cells = row.find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                # Check for specific cases early to set the location
                if len(cell_data) == 4 and "Market" in cell_data[0]:
                    location = "USA"

                # Continue if basic criteria aren't met
                if location and cell_data.count("\xa0") < 4 and len(cell_data) > 3 and "@" in cell_data[3]:

                    # Set location from cell_data if criteria met
                    if "\xa0" not in cell_data[0]:
                        location = cell_data[0]

                    # Extract size and term information
                    size = cell_data[1].split(" ")[0]
                    term = cell_data[1].replace(size, '').strip()

                    # Format size based on term content
                    if "ST" in term:
                        size += " ST"
                        term = term.replace("ST", '').strip()
                    
                    # Determine grade
                    grade_data = {"D.D.": "DD", "S.D.": "SD", "O.S.": "OS"}
                    grade = ""
                    for key, value in grade_data.items():
                        if key in term:
                            grade = value
                            term = term.replace(key, '').strip()

                    # Clean up `size` and format quantity, price
                    size = size.replace("'", "&#39;")
                    quantity, price = cell_data[2], ''
                    if "$" in quantity:
                        quantity, price = quantity.split(" x ")[0], cell_data[2].split("$")[1].replace(',', '')

                    # Extract feature and depot information
                    feature, depot = cell_data[3].split("@")[0], cell_data[3].split("@")[1]

                    # Build and execute SQL insert statement
                    insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '', '', 'Jeff Young, Lummid Containers', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                    execute_query(conn, insert)
            
            return

        # ---------------  Parsing for westcoast@lummid.com (Daniel Callaway, Lummid Containers) --------------- #
        case "westcoast@lummid.com":
            location = ""
            for row in rows:
                cells = row.find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                # Check for specific cases early to set the location
                if len(cell_data) == 5 and "Market" in cell_data[0]:
                    location = "CANADA"

                # Continue if basic criteria aren't met
                if location and cell_data.count("\xa0") < 5 and len(cell_data) > 4 and len(cell_data[2]) < 3:

                    # Set location from cell_data if criteria met
                    if "\xa0" not in cell_data[0]:
                        location = cell_data[0]

                    # Extract size and term information
                    size = cell_data[1].split(" ")[0].replace("'", "&#39;") + cell_data[1].split(" ")[1]
                    term = cell_data[1].split(" ")[-1].strip()

                    grade = ""
                    if "USED" in cell_data[1]:
                        grade = "USED"

                    quantity, price = cell_data[2], cell_data[3].replace('$', '').replace(',', '')
                    feature = cell_data[4].replace('\xa0', '')

                    # Build and execute SQL insert statement
                    insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '', 'Daniel Callaway, Lummid Containers', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                    execute_query(conn, insert)
            
            return
        
        # ---------------  Parsing for ryanchoi@muwon.com (Ryan Jongwon Choi, MUWON USA) --------------- #
        case "ryanchoi@muwon.com":
            size_data = ["20&#39;GP", "40&#39;GP", "40&#39;HC", "20&#39;GP", "40&#39;HC"]
            type_data = ["Used", "Used", "Used", "New", "New"]
            term_data = ["Cargo Worthy", "Cargo Worthy", "Cargo Worthy", "One-Trip", "One-Trip"]

            for i in range(2, len(rows)):
                cells = rows[i].find_all('td')
                cell_data = [cell.get_text() for cell in cells]

                if len(cell_data) > 5:
                    location = cell_data[1] + ", " + cell_data[0]
                    depot = cell_data[2] if len(cell_data) > 7 else ""

                    start_index = 3 if len(cell_data) > 7 else 2
                    for j in range(start_index, len(cell_data)):
                        if "\xa0" not in cell_data[j]:
                            quantity = cell_data[j].replace('(', '').replace(')', '')
                            feature = "GATE BUY Available" if "(" in cell_data[j] else ""
                            size = size_data[j - start_index]
                            type = type_data[j - start_index]
                            term = term_data[j - start_index]
                            
                            # Build and execute SQL insert statement
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '{type}', '{term}', '{location}', '', '', '{feature}', '{depot}', '', '', 'Daniel Callaway, Lummid Containers', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)
                
            return

    # Close the connection
    if conn:
        conn.close()

    return body

def get_message_content_plain(service, message_id):
    """Retrieve a specific message by its ID and decode its content."""
    message = service.users().messages().get(userId='me', id=message_id, format='raw').execute()
    msg_raw = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
    email_message = message_from_bytes(msg_raw)

    # Replace the following variables with your database credentials
    host = "localhost"
    user = "root"
    password = ""
    database = "container"

    # Create a connection
    conn = create_connection(host, user, password, database)

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
    received_date = email_message['Date']
    subject = email_message['Subject']
    current_datetime = datetime.now()
    created_date = current_datetime.strftime("%Y/%m/%d %H:%M:%S")

    content = re.sub(r"^\s*$\n", "", body, flags=re.MULTILINE)
    
    print(subject)
    print(vendor_email[0])
    # print(content)

    match vendor_email[0]:
        # ---------------  Parsing for rolly@oceanbox.cn (Rolly, Oceanbox logistic limited) --------------- #
        case "rolly@oceanbox.cn":
            if "inventory" in subject:
                content_data = content.split("Thank you!")[1].split("Container expert from China")[0].split("Note")[0].split("\n")
                location = ''
                for i in range(0, len(content_data)):
                    if len(content_data[i].split(",")) < 3:
                        location = content_data[i]
                    else:
                        content_data[i] = content_data[i].replace("，", ",")
                        if len(content_data[i].split(",")) == 5:
                            if "$" in content_data[i].split(",")[3] and "," in content_data[i].split("$")[1]:
                                grade = ''
                                depot = ''
                                eta = ''
                                quantity = content_data[i].split(" x ")[0]
                                size = content_data[i].split(",")[0].split(" x ")[1]
                                if "DD" in size:
                                    grade = "DD"
                                    size = size.replace("DD", '')
                                if "OS" in size:
                                    grade = "OS"
                                    size = size.replace("OS", '')
                                    if "Full Open Side" in size:
                                        grade = "OS Full Open Side"
                                        size = size.replace("Full Open Side", '')

                                if "ETA" in content_data[i].split(",")[4]:
                                    eta = content_data[i].split(",")[4].replace(" ", "", 1)
                                else:
                                    depot = content_data[i].split(",")[4].replace(" ", "", 1)

                                term = content_data[i].split(",")[1].replace(" ", "", 1)
                                feature = content_data[i].split(",")[2].replace(" ", "", 1)
                                price = content_data[i].split(",")[3].replace("$", '').replace(" ", "", 1)
                                full_line = content_data[i]

                                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                                execute_query(conn, insert)
                            if "$" not in content_data[i]:
                                grade = ''
                                depot = ''
                                eta = ''
                                quantity = content_data[i].split(" x ")[0]
                                size = content_data[i].split(",")[0].split(" x ")[1]
                                if "DD" in size:
                                    grade = "DD"
                                    size = size.replace("DD", '')
                                if "OS" in size:
                                    grade = "OS"
                                    size = size.replace("OS", '')

                                grade = grade + "" + content_data[i].split(",")[1]

                                if "ETA" in content_data[i].split(",")[4]:
                                    eta = content_data[i].split(",")[4].replace(" ", "", 1)
                                else:
                                    depot = content_data[i].split(",")[4].replace(" ", "", 1)

                                term = content_data[i].split(",")[2].replace(" ", "", 1)
                                feature = content_data[i].split(",")[3].replace(" ", "", 1)
                                
                                full_line = content_data[i]

                                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                                execute_query(conn, insert)
                        
                        if len(content_data[i].split(",")) == 6 and "$" in content_data[i].split(",")[3] and "," in content_data[i].split("$")[1]:
                            grade = ''
                            depot = ''
                            eta = ''
                            quantity = content_data[i].split(" x ")[0]
                            size = content_data[i].split(",")[0].split(" x ")[1]
                            term = content_data[i].split(",")[1].replace(" ", "", 1)
                            if "DD" in size:
                                grade = "DD"
                                size = size.replace("DD", '')
                            if "OS" in size:
                                grade = "OS"
                                size = size.replace("OS", '')

                            if content_data[i].count("$") == 3:
                                feature = ''
                                depot = content_data[i].split(",")[2].replace(" ", "", 1)
                                price = content_data[i].split(",")[3].replace("$", '') + "," + content_data[i].split(",")[4].replace("$", '') + "," + content_data[i].split(",")[5].replace("$", '')
                                price = price.replace(" ", "", 1)
                                full_line = content_data[i]

                                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                                execute_query(conn, insert)
                            else:
                                if "Full open side" in content_data[i].split(",")[4]:
                                    grade = grade + " Full open side"
                                    depot = content_data[i].split(",")[5].replace(" ", "", 1)
                                else:
                                    depot = content_data[i].split(",")[4].replace(" ", "", 1).replace("(", "")
                                    eta = content_data[i].split(",")[5].replace(" ", "", 1).replace(")", "")

                                feature = content_data[i].split(",")[2].replace(" ", "", 1)
                                price = content_data[i].split(",")[3].replace("$", '').replace(" ", "", 1)
                                full_line = content_data[i]

                                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                                execute_query(conn, insert)

                        if len(content_data[i].split(",")) == 6 and content_data[i].count("$") == 1 and "$" in content_data[i].split(",")[4] and "," in content_data[i].split("$")[1]:
                            grade = ''
                            depot = ''
                            eta = ''
                            quantity = content_data[i].split(" x ")[0]
                            size = content_data[i].split(",")[0].split(" x ")[1]
                            if "DD" in size:
                                grade = "DD"
                                size = size.replace("DD", '')
                            if "OS" in size:
                                grade = "OS"
                                size = size.replace("OS", '')
                            
                            if "door" in content_data[i].split(",")[1] or "full open side" in content_data[i].split(",")[1]:
                                grade = grade + " " + content_data[i].split(",")[1]
                                term = content_data[i].split(",")[2].replace(" ", "", 1)
                                feature = content_data[i].split(",")[3].replace(" ", "", 1)
                                depot = content_data[i].split(",")[5].replace(" ", "", 1)

                            elif "door" in content_data[i].split(",")[2] or "full open side" in content_data[i].split(",")[2]:
                                grade = grade + " " + content_data[i].split(",")[2]
                                term = content_data[i].split(",")[1].replace(" ", "", 1)
                                feature = content_data[i].split(",")[3].replace(" ", "", 1)
                                depot = content_data[i].split(",")[5].replace(" ", "", 1)
                            else:
                                if "ETA" in content_data[i].split(",")[5]:
                                    eta = content_data[i].split(",")[5].replace(" ", "", 1)
                                    depot = content_data[i].split(",")[3].replace(" ", "", 1)
                                elif "ETA" in content_data[i].split(",")[3]:
                                    eta = content_data[i].split(",")[3].replace(" ", "", 1)
                                    depot = content_data[i].split(",")[5].replace(" ", "", 1)
                                else:
                                    depot = content_data[i].split(",")[5].replace(" ", "", 1)

                                if "full open side" in content_data[i]:
                                    grade = grade + " full open side"
                                    depot = content_data[i].split(",")[3].replace(" ", "", 1)
                                
                                term = content_data[i].split(",")[1].replace(" ", "", 1)
                                feature = content_data[i].split(",")[2].replace(" ", "", 1)
                            
                            if "YOM" in content_data[i]:
                                term = content_data[i].split(",")[1].replace(" ", "", 1)
                                feature = content_data[i].split(",")[2].replace(" ", "", 1) + "," + content_data[i].split(",")[3]
                                depot = content_data[i].split(",")[5].replace(" ", "", 1)

                            price = content_data[i].split(",")[4].replace("$", '').replace(" ", "", 1)
                            full_line = content_data[i]

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                        if len(content_data[i].split(",")) >= 7:
                            if "$" in content_data[i].split(",")[3]:
                                grade = ''
                                depot = ''
                                eta = ''
                                quantity = content_data[i].split(" x ")[0]
                                size = content_data[i].split(",")[0].split(" x ")[1]
                                if "DD" in size:
                                    grade = "DD"
                                    size = size.replace("DD", '')
                                if "OS" in size:
                                    grade = "OS"
                                    size = size.replace("OS", '')

                                if len(content_data[i].split(",")) == 7:
                                    depot = content_data[i].split(",")[4].replace(" ", "", 1) + "," + content_data[i].split(",")[5]
                                    eta = content_data[i].split(",")[6].replace(" ", "", 1)
                                
                                if len(content_data[i].split(",")) == 9:
                                    depot = content_data[i].split(",")[4].replace(" ", "", 1) + "," + content_data[i].split(",")[5] + "," + content_data[i].split(",")[6] + "," + content_data[i].split(",")[7]
                                    eta = content_data[i].split(",")[8].replace(" ", "", 1)
                                    
                                term = content_data[i].split(",")[1].replace(" ", "", 1)
                                feature = content_data[i].split(",")[2].replace(" ", "", 1)
                                price = content_data[i].split(",")[3].replace("$", '').replace(" ", "", 1)
                                full_line = content_data[i]

                                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                                execute_query(conn, insert)

                            if "$" in content_data[i].split(",")[4]:
                                grade = ''
                                depot = ''
                                eta = ''
                                quantity = content_data[i].split(" x ")[0]
                                size = content_data[i].split(",")[0].split(" x ")[1]
                                if "DD" in size:
                                    grade = "DD"
                                    size = size.replace("DD", '')
                                if "OS" in size:
                                    grade = "OS"
                                    size = size.replace("OS", '')

                                if "doors" in content_data[i] or "full open side" in content_data[i]:
                                    grade = grade + " " + content_data[i].split(",")[1]
                                    term = content_data[i].split(",")[2].replace(" ", "", 1) 
                                    feature = content_data[i].split(",")[3]
                                    depot = content_data[i].split(",")[5].replace(" ", "", 1) + "," + content_data[i].split(",")[6]
                                else:
                                    term = content_data[i].split(",")[1].replace(" ", "", 1) 
                                    feature = content_data[i].split(",")[2].replace(" ", "", 1)
                                    depot = content_data[i].split(",")[3].replace(" ", "", 1) + "," + content_data[i].split(",")[5] + "," + content_data[i].split(",")[6]

                                price = content_data[i].split(",")[4].replace("$", '').replace(" ", "", 1)
                                full_line = content_data[i]

                                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                                execute_query(conn, insert)

                            if "$" in content_data[i].split(",")[6]:
                                grade = ''
                                depot = ''
                                eta = ''
                                quantity = content_data[i].split(" x ")[0]
                                size = content_data[i].split(",")[0].split(" x ")[1]
                                if "DD" in size:
                                    grade = "DD"
                                    size = size.replace("DD", '')
                                if "OS" in size:
                                    grade = "OS"
                                    size = size.replace("OS", '')
                                
                                if "doors" in content_data[i] or "full open side" in content_data[i]:
                                    grade = grade + " " + content_data[i].split(",")[1] + "" + content_data[i].split(",")[2]
                                    term = content_data[i].split(",")[3].replace(" ", "", 1) 
                                    feature = content_data[i].split(",")[4].replace(" ", "", 1)
                                    depot = content_data[i].split(",")[5].replace(" ", "", 1)
                                else:
                                    term = content_data[i].split(",")[1].replace(" ", "", 1) 
                                    feature = content_data[i].split(",")[2].replace(" ", "", 1) + "," + content_data[i].split(",")[3]
                                    depot = content_data[i].split(",")[4].replace(" ", "", 1) + "," + content_data[i].split(",")[5]

                                
                                price = content_data[i].split(",")[6].replace("$", '').replace(" ", "", 1)
                                full_line = content_data[i]

                                insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                                execute_query(conn, insert)

                        if "$" in content_data[i].split(",")[2]:
                            term = ''
                            grade = ''
                            depot = ''
                            eta = ''
                            feature = ''
                            if "RAL" in content_data[i]:
                                quantity = content_data[i].split(" x ")[0]
                                size = content_data[i].split(",")[0].split(" x ")[1]
                                if "DD" in size:
                                    grade = "DD"
                                    size = size.replace("DD", '')
                                if "OS" in size:
                                    grade = "OS"
                                    size = size.replace("OS", '')
                                depot = content_data[i].split(",")[3].replace(" ", "", 1)
                                feature = content_data[i].split(",")[1].replace(" ", "", 1)
                                price = content_data[i].split(",")[2].replace("$", '').replace(" ", "", 1)
                                full_line = content_data[i]
                            else:
                                quantity = content_data[i].split(" x ")[0]
                                size = content_data[i].split(",")[0].split(" x ")[1]
                                term = content_data[i].split(",")[1].replace(" ", "", 1)
                                if "DD" in size:
                                    grade = "DD"
                                    size = size.replace("DD", '')
                                if "OS" in size:
                                    grade = "OS"
                                    size = size.replace("OS", '')
                                
                                if "ETA" in content_data[i].split(",")[3]:
                                    eta = content_data[i].split(",")[3].replace(" ", "", 1)
                                else:
                                    depot = content_data[i].split(",")[3].replace(" ", "", 1)

                                if len(content_data[i].split(",")) == 5:
                                    depot = content_data[i].split(",")[3] + "," + content_data[i].split(",")[4]
                                    depot = depot.replace(" ", "", 1)
                                
                                price = content_data[i].split(",")[2].replace("$", '').replace(" ", "", 1)
                                full_line = content_data[i]

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                        if len(content_data[i].split(",")) == 4 and "$" in content_data[i].split(",")[3] and content_data[i].count("$") == 1:
                            grade = ''
                            depot = ''
                            eta = ''
                            quantity = content_data[i].split(" x ")[0]
                            size = content_data[i].split(",")[0].split(" x ")[1]

                            if "DD" in size:
                                grade = "DD"
                                size = size.replace("DD", '')
                            if "OS" in size:
                                grade = "OS"
                                size = size.replace("OS", '')

                            
                            price = content_data[i].split(",")[3].replace("$", '').replace(" ", "", 1)

                            term = content_data[i].split(",")[1].replace(" ", "", 1) 
                            feature = content_data[i].split(",")[2].replace(" ", "", 1)
                            
                            full_line = content_data[i]

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                        if len(content_data[i].split(",")) == 5 and "$" in content_data[i].split(",")[4] and content_data[i].count("$") == 1:
                            grade = ''
                            depot = ''
                            eta = ''
                            quantity = content_data[i].split(" x ")[0]
                            size = content_data[i].split(",")[0].split(" x ")[1]
                            if "DD" in size:
                                grade = "DD"
                                size = size.replace("DD", '')
                            if "OS" in size:
                                grade = "OS"
                                size = size.replace("OS", '')
                            
                            if "doors" in content_data[i] or "full open side" in content_data[i]:
                                grade = grade + " " + content_data[i].split(",")[1]
                                if "RAL" in content_data[i].split(",")[2]:
                                    term = ''
                                    feature = content_data[i].split(",")[2].replace(" ", "", 1)
                                    depot = content_data[i].split(",")[3].replace(" ", "", 1)
                                else:
                                    term = content_data[i].split(",")[2].replace(" ", "", 1)
                                    if "ETA" in content_data[i].split(",")[3]:
                                        feature = content_data[i].split(",")[3].split("ETA")[0].replace(" ", "", 1)
                                        eta = "ETA" + content_data[i].split(",")[3].split("ETA")[1].replace(" ", "", 1)
                                    else:
                                        feature = content_data[i].split(",")[3].replace(" ", "", 1)
                            else: 
                                if "ETA" in content_data[i].split(",")[3]:
                                    eta = content_data[i].split(",")[3].replace(" ", "", 1)
                                else:
                                    depot = content_data[i].split(",")[3].replace(" ", "", 1)
                                term = content_data[i].split(",")[1].replace(" ", "", 1) 
                                feature = content_data[i].split(",")[2].replace(" ", "", 1)

                            price = content_data[i].split(",")[4].replace("$", '').replace(" ", "", 1)
                            full_line = content_data[i]

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                        if len(content_data[i].split(",")) == 6 and "$" in content_data[i].split(",")[5] and content_data[i].count("$") == 1:
                            grade = ''
                            depot = ''
                            eta = ''
                            quantity = content_data[i].split(" x ")[0]
                            size = content_data[i].split(",")[0].split(" x ")[1]
                            if "DD" in size:
                                grade = "DD"
                                size = size.replace("DD", '')
                            if "OS" in size:
                                grade = "OS"
                                size = size.replace("OS", '')
                            
                            if "doors" in content_data[i] or "full open side" in content_data[i]:
                                grade = grade + " " + content_data[i].split(",")[1]
                                term = content_data[i].split(",")[2].replace(" ", "", 1) 
                                feature = content_data[i].split(",")[3].replace(" ", "", 1)
                                depot = content_data[i].split(",")[4].replace(" ", "", 1)
                            else:
                                term = content_data[i].split(",")[1].replace(" ", "", 1) 
                                feature = content_data[i].split(",")[2].replace(" ", "", 1) + "," + content_data[i].split(",")[3]
                                depot = content_data[i].split(",")[4].replace(" ", "", 1)

                            
                            price = content_data[i].split(",")[5].replace("$", '').replace(" ", "", 1)
                            full_line = content_data[i]

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                        if "$" not in content_data[i]:
                            grade = ''
                            depot = ''
                            eta = ''
                            quantity = content_data[i].split(" x ")[0]
                            size = content_data[i].split(",")[0].split(" x ")[1]
                            if "DD" in size:
                                grade = "DD"
                                size = size.replace("DD", '')
                            if "OS" in size:
                                grade = "OS"
                                size = size.replace("OS", '')
                            
                            if "doors" in content_data[i] or "full open side" in content_data[i]:
                                grade = grade + " " + content_data[i].split(",")[1]
                                term = content_data[i].split(",")[2].replace(" ", "", 1) 
                                feature = content_data[i].split(",")[3].replace(" ", "", 1)
                                depot = content_data[i].split(",")[4].replace(" ", "", 1)
                            else:
                                term = content_data[i].split(",")[1].replace(" ", "", 1) 
                                feature = content_data[i].split(",")[2].replace(" ", "", 1)
                                depot = content_data[i].split(",")[3].replace(" ", "", 1)

                            full_line = content_data[i]

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '', '{feature}', '{depot}', '{eta}', '{full_line}', 'Rolly, Oceanbox logistic limited', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

            return

        # ---------------  Parsing for Bryan@scontainers.com (Bryan Lucas, STAR CONTAINER SOLUTION) --------------- #
        case "Bryan@scontainers.com":
            content_data = content.split("\n")
            location = ''
            for i in range(0, len(content_data)):
                if "*" in content_data[i] and "$" not in content_data[i] and "ETA" not in content_data[i]:
                    location = content_data[i].replace("*", '')
                    
                if "$" in content_data[i]:
                    if "CW" in content_data[i]:
                        quantity = content_data[i].replace("*", '').split("X")[0].replace(" ", '')
                        size = content_data[i].replace("*", '').split("X")[1].split(" CW")[0].replace("'", '&#39;')
                        feature = content_data[i].split(" - ")[0].split("CW")[1]
                        term = "CW"
                        price = content_data[i].replace("*", '').split(" - ")[1].replace("$", '').replace(",", '')
                        full_line = content_data[i].replace("'", '&#39;').replace("*", '')

                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '', '{price}', '{feature}', '', '', '{full_line}', 'Bryan Lucas, STAR CONTAINER SOLUTION', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

                    elif "ONE TRIP" in content_data[i]:
                        if "OPEN SIDE" in content_data[i]:
                            quantity = content_data[i].replace("*", '').split("X")[0].replace(" ", '')
                            feature = content_data[i].split("(")[1].split(")")[0]
                            size = content_data[i].replace("*", '').split("X")[1].split("OPEN SIDE")[0].replace(" ", '').replace("'", '&#39;')
                            grade = "OPEN SIDE"
                            term = "ONE TRIP"
                            price = content_data[i].replace("*", '').split(" - ")[1].replace("$", '').replace(",", '')
                            full_line = content_data[i].replace("'", '&#39;').replace("*", '')

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '{full_line}', 'Bryan Lucas, STAR CONTAINER SOLUTION', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                        elif "DD" in content_data[i]:
                            quantity = content_data[i].replace("*", '').split("X")[0].replace(" ", '')
                            feature = content_data[i].split("(")[1].split(")")[0]
                            size = content_data[i].replace("*", '').split("X")[1].split("DD")[0].replace(" ", '').replace("'", '&#39;')
                            grade = "DD"
                            term = "ONE TRIP"
                            price = content_data[i].replace("*", '').split(" - ")[1].replace("$", '').replace(",", '')
                            full_line = content_data[i].replace("'", '&#39;').replace("*", '')

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '{full_line}', 'Bryan Lucas, STAR CONTAINER SOLUTION', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                        else:
                            quantity = content_data[i].replace("*", '').split("X")[0].replace(" ", '')
                            feature = content_data[i].split("(")[1].split(")")[0]
                            size = content_data[i].replace("*", '').split("X")[1].split("ONE TRIP")[0].replace(" ", '').replace("'", '&#39;')
                            term = "ONE TRIP"
                            price = content_data[i].replace("*", '').split(" - ")[1].replace("$", '').replace(",", '')
                            full_line = content_data[i].replace("'", '&#39;').replace("*", '')

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '', '{price}', '{feature}', '', '', '{full_line}', 'Bryan Lucas, STAR CONTAINER SOLUTION', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)
                        
                    elif "2-3 TRIPS" in content_data[i] or "NEWER" in content_data[i]:
                        quantity = content_data[i].replace("*", '').split("X")[0].replace(" ", '')
                        feature = content_data[i].split("(")[1].split(")")[0].replace("2-3 TRIPS; ", '')
                        size = content_data[i].replace("*", '').split("X")[1].split("IICL")[0].replace(" ", '').replace("'", '&#39;')
                        grade = "IICL"
                        term = ''
                        if "2-3 TRIPS" in content_data[i]:
                            term = "2-3 TRIPS"
                        if "NEWER" in content_data[i]:
                            term = "NEWER"
                        price = content_data[i].replace("*", '').split(" - ")[1].replace("$", '').replace(",", '')
                        full_line = content_data[i].replace("'", '&#39;').replace("*", '')

                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '{full_line}', 'Bryan Lucas, STAR CONTAINER SOLUTION', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)
                    
                    elif "AS IS" in content_data[i] or "WWT" in content_data[i]:
                        term = ''
                        if "AS IS" in content_data[i]:
                            term = "AS IS"
                        if "WWT" in content_data[i]:
                            term = "WWT"
                        term_temp = term + " "
                        quantity = content_data[i].replace("*", '').split("X")[0].replace(" ", '')
                        feature = content_data[i].split(term_temp)[1].split(" - ")[0].replace(" ", '')
                        size = content_data[i].replace("*", '').split("X")[1].split(term)[0].replace(" ", '').replace("'", '&#39;')
                        price = content_data[i].replace("*", '').split(" - ")[1].replace("$", '').replace(",", '')
                        full_line = content_data[i].replace("'", '&#39;').replace("*", '')

                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '', '{price}', '{feature}', '', '', '{full_line}', 'Bryan Lucas, STAR CONTAINER SOLUTION', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)

            return
        
        # ---------------  Parsing for jenny@icc-solution.com (Jenny Roberts, International Container & Chassis Solution) --------------- #
        case "jenny@icc-solution.com":
            content_data_temp = content.split("Regards,")[0].split("\n")
            content_data = []
            location = ""

            for item in content_data_temp:
                item = item.replace("\r", "")
                if " x" in item or "*" in item or "+" in item:
                    if " x" in item and "*" in item:
                        content_data.append("*" + item.split("*")[1] + "*")
                        content_data.append(item.split("*")[2])
                    elif "each8 x 40" not in item:
                        content_data.append(item)

                elif len(content_data) > 0:
                    if "'" in item:
                        content_data.append(item)
                    else:
                        content_data[-1] = content_data[-1] + " " + item
                    
                    if "each8 x 40" in item:
                        content_data[-1] = content_data[-1] + " " + item.split("each")[0] + "each"
                        content_data.append(item.split("each")[1])
            length = 0
            for item in content_data:
                if "*" in item:
                    location = item.replace("*", '')
                    length += 1
                else:
                    if "CW" in item or "WWT" in item or "IICL" in item:
                        type = ''
                        grade = ''
                        feature = ''
                        if " x" in item:
                            quantity = item.split(" x")[0]
                            size = item.split(" x")[1].lstrip().split(" ")[0].replace("'", '&#39;')
                        else:
                            quantity = '1'
                            size = item.split(" ")[0].replace("'", '&#39;')

                        if "DD" in size:
                            grade = "DD"
                            size = size.replace("DD", '')
                        if "OS" in size:
                            grade = "OS"
                            size = size.replace("OS", '')

                        if "Used" in item:
                            type = "Used"
                        if "Newer" in item:
                            type = "Newer"
                            if ";" in item and len(item.split(";")) > 2:
                                feature = item.split(";")[1].replace(" ", "", 1) + "," + item.split(";")[2]
                        
                        terms = ["CW", "WWT", "IICL"]
                        term = next((t for t in terms if t in item), None)

                        price = item.split("$")[1].replace(",", "")
                        full_line = item.replace("'", '&#39;')
                        
                        insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '{type}', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '{full_line}', 'Jenny Roberts, International Container & Chassis Solution', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                        execute_query(conn, insert)
                        
                    else:          
                        if len(item.split(";")) == 2:
                            
                            if " x" in item:
                                quantity = item.split(" x")[0]
                                if "New" in item:
                                    size = item.split(" x")[1].split("New")[0].strip().replace("'", '&#39;')
                                    term = "New/One Trip"
                            elif "+" in item:
                                size = item.split(")")[1].split("New")[0].strip().replace("'", '&#39;')
                                quantity = item.split(")")[0].replace("(", "")
                                term = "New/One Trip"

                            # List of (term, grade) pairs to check
                            grade = ''
                            terms = ["DD", "OS", "Duocon", "Full Open Side", "Open Side", "Open Side (4 Doors)", "Side Door (4 doors with posts)"]  
                            # Loop through each term and update grade and size if the term is found
                            for term in terms:  
                                if term in size:  
                                    grade = term  
                                    size = size.replace(term, '').strip()
                                    break  # Exit the loop once a match is found

                            price = item.split("$")[1].replace(",", "")
                            full_line = item.replace("'", '&#39;')

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '{full_line}', 'Jenny Roberts, International Container & Chassis Solution', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                        if len(item.split(";")) == 3:
                            grade = ''
                            if " x" in item:
                                quantity = item.split(" x")[0]
                                size = item.split(" x")[1].split("New")[0].strip().replace("'", '&#39;')
                                term = "New/One Trip"

                            if "DD" in size:
                                grade = "DD"
                                size = size.replace("DD", '')
                            if "OS" in size:
                                grade = "OS"
                                size = size.replace("OS", '')
                            if "standard" in size:
                                grade = "standard"
                                size = size.replace("standard", '')

                            feature = item.split(";")[1].strip()
                            if len(item.split(";")[2].split("$")[0]) > 3:
                                feature = feature + item.split(";")[2].split("$")[0]
                            
                            if len(item.split(";")[0].split("Trip")[1]) > 3:
                                feature = feature + item.split(";")[0].split("Trip")[1]
                            
                            price = item.split("$")[1].replace(",", "")
                            full_line = item.replace("'", '&#39;')

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '{full_line}', 'Jenny Roberts, International Container & Chassis Solution', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)
                        
                        if len(item.split(";")) == 4:
                            grade = ''
                            if " x" in item:
                                quantity = item.split(" x")[0]
                                if "New" in item:
                                    size = item.split(" x")[1].split("New")[0].strip().replace("'", '&#39;')
                                    term = "New/One Trip"
                                elif "2-3" in item:
                                    size = item.split(" x")[1].split("2-3")[0].strip().replace("'", '&#39;')
                                    quantity = item.split(" x")[0]
                                    term = "2-3 Trips"
                            elif "+" in item:
                                size = item.split(")")[1].split("New")[0].strip().replace("'", '&#39;')
                                quantity = item.split(")")[0].replace("(", "")
                                term = "New/One Trip"

                            # List of (term, grade) pairs to check
                            grade = ''
                            terms = ["DD", "OS", "Duocon", "Full Open Side", "Open Side", "Open Side (4 Doors)", "Side Door (4 doors with posts)"]  
                            # Loop through each term and update grade and size if the term is found
                            for term in terms:  
                                if term in size:  
                                    grade = term  
                                    size = size.replace(term, '').strip()
                                    break  # Exit the loop once a match is found

                            feature = item.split(";")[1].strip() + "," + item.split(";")[2]
                            price = item.split("$")[1].replace(",", "")
                            full_line = item.replace("'", '&#39;')
                            
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '{full_line}', 'Jenny Roberts, International Container & Chassis Solution', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)
                        
                        if len(item.split(";")) == 5:
                            if " x" in item:
                                size = item.split(" x")[1].split("New")[0].strip().replace("'", '&#39;')
                                quantity = item.split(" x")[0]
                                term = "New/One Trip"
                            
                            # List of (term, grade) pairs to check
                            grade = ''
                            terms = ["DD", "OS", "Duocon", "Open Side (4 Doors)"]  
                            # Loop through each term and update grade and size if the term is found
                            for term in terms:  
                                if term in size:  
                                    grade = term  
                                    size = size.replace(term, '').strip()
                                    break  # Exit the loop once a match is found

                            feature = item.split(";")[1].strip() + "," + item.split(";")[2] + "," + item.split(";")[3]
                            price = item.split("$")[1].replace(",", "")
                            full_line = item.replace("'", '&#39;')
                            
                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '', '', '{full_line}', 'Jenny Roberts, International Container & Chassis Solution', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

                        if len(item.split(";")) == 6:
                            size = item.split(" x")[1].split("New")[0].strip().replace("'", '&#39;')
                            quantity = item.split(" x")[0]
                            term = "New/One Trip"

                            feature = item.split(";")[1].strip() + "," + item.split(";")[2] + "," + item.split(";")[3] + "," + item.split(";")[4]
                            depot = item.split("each")[1].strip()
                            price = item.split("$")[1].split("(")[0].replace(",", "")
                            full_line = item.replace("'", '&#39;')

                            insert = f"INSERT INTO container (size, quantity, type, term, location, grade, price, feature, depot, ETA, full_line, provider, vendor, received_date, created_date) VALUES ('{size}', '{quantity}', '', '{term}', '{location}', '{grade}', '{price}', '{feature}', '{depot}', '', '{full_line}', 'Jenny Roberts, International Container & Chassis Solution', '{vendor_email[0]}', '{received_date}', '{created_date}')"
                            execute_query(conn, insert)

    # Close the connection
    if conn:
        conn.close()

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

    # current_datetime = datetime.now()
    # today = current_datetime.strftime("%Y/%m/%d")
    # query = f"after:{today}"

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

    # query = 'from:john@americanacontainers.com after:2024/10/23'
    # query = 'from:tine@americanacontainers.com after:2024/10/28'
    # query = 'from:johannes@oztradingltd.com after:2024/10/28'
    # query = 'from:steven.gao@cgkinternational.com after:2024/7/8'
    # query = 'from:sales@isr-containers.com subject:ISR Containers: Monday Blast! after:2023/5/15'
    # query = 'from:wayne.vandenburg@dutchcontainers.com after:2024/10/22'
    # query = 'from:wayne.vandenburg@trident-containers.com after:2023/10/16'
    # query = 'from:ryan@trident-containers.com after:2024/7/8'
    # query = 'from:e4.mevtnhrict@gcc2011.com after:2024/10/21'
    # query = 'from:e8.pa@gcc2011.com after:2022/12/1'
    # query = 'from:e61.md@gcc2011.com after:2023/10/17'
    # query = 'from:W3.Wa@gcc2011.com after:2024/10/24'
    # query = 'from:W6.CaLgb@gcc2011.com after:2023/10/16'
    # query = 'from:W8.CaLgb@gcc2011.com after:2023/10/9'
    # query = 'from:c6.wi@gcc2011.com after:2023/10/17'
    # query = 'from:c17.txelp@gcc2011.com after:2024/10/21'
    # query = 'from:m1.ntab@gcc2011.com after:2023/10/17'
    # query = 'from:ash@container-xchange.com after:2024/10/21'
    # query = 'from:Saquib.amiri@sadecontainers.com after:2024/10/22'
    # query = 'from:JAnguish@ism247.com after:2024/10/28'
    # query = 'from:sales@tritoncontainersales.com after:2024/10/21'
    # query = 'from:thomas@fulidacontainer.com after:2024/7/2'
    # query = 'from:magui.cheung@northatlanticcontainer.com after:2024/10/28'
    # query = 'from:jeff@lummid.com after:2024/7/8'
    # query = 'from:eastcoast@lummid.com after:2024/10/7'
    # query = 'from:westcoast@lummid.com after:2024/10/29'
    # query = 'from:rolly@oceanbox.cn after:2024/11/4'
    # query = 'from:Bryan@scontainers.com subject:Units available after:2024/7/1'
    # query = 'from:jenny@icc-solution.com subject:Halloween SALE/ after:2023/10/31'
    # query = 'from:alex@icc-solution.com subject:Halloween SALE/ after:2023/10/31'
    # query = 'from:ryanchoi@muwon.com after:2023/10/2'

    query_html_lists = [    
                            "from:john@americanacontainers.com after:2024/10/28",
                            "from:tine@americanacontainers.com after:2024/10/28",
                            "from:johannes@oztradingltd.com after:2024/11/4",
                            "from:steven.gao@cgkinternational.com after:2024/7/8",
                            "from:sales@isr-containers.com subject:ISR Containers: Monday Blast! after:2023/5/15",
                            "from:wayne.vandenburg@dutchcontainers.com after:2024/10/22",
                            "from:wayne.vandenburg@trident-containers.com after:2023/10/16",
                            "from:ryan@trident-containers.com after:2024/7/8",
                            "from:e4.mevtnhrict@gcc2011.com after:2024/10/22",
                            "from:e8.pa@gcc2011.com after:2024/10/25",
                            "from:e61.md@gcc2011.com after:2023/10/17",
                            "from:W3.Wa@gcc2011.com after:2024/10/24",
                            "from:W6.CaLgb@gcc2011.com after:2023/10/16",
                            "from:W8.CaLgb@gcc2011.com after:2023/10/9",
                            "from:c6.wi@gcc2011.com after:2023/10/17",
                            "from:c17.txelp@gcc2011.com after:2024/10/21",
                            "from:m1.ntab@gcc2011.com after:2023/10/17",
                            "from:ash@container-xchange.com after:2024/11/5",
                            "from:Saquib.amiri@sadecontainers.com after:2024/10/28",
                            "from:JAnguish@ism247.com after:2024/11/4",
                            "from:sales@tritoncontainersales.com after:2024/10/21",
                            "from:thomas@fulidacontainer.com after:2024/11/5",
                            "from:magui.cheung@northatlanticcontainer.com after:2024/11/1",
                            "from:jeff@lummid.com after:2024/7/8",
                            "from:eastcoast@lummid.com after:2024/11/4",
                            "from:westcoast@lummid.com after:2024/10/29",
                            "from:ryanchoi@muwon.com after:2023/10/16"
                        ]

    query_plain_lists = [   
                            "from:rolly@oceanbox.cn after:2024/11/4",
                            "from:Bryan@scontainers.com subject:Units available after:2024/7/1",
                            "from:jenny@icc-solution.com subject:Halloween SALE/ after:2024/10/31"
                        ]

    # get_today_emails()
    
    # current_datetime = datetime.now()
    # yesterday = current_datetime - timedelta(days=1)
    # yesterday_str = yesterday.strftime("%Y/%m/%d")
    # query = f"after:{yesterday_str}"
    
    # messages = get_messages(service, query=query)
    # if messages:
    #     for message in messages:
    #         get_message_content_plain(service, message['id'])

    for query_html_list in query_html_lists:
        # Get the messages matching the query
        messages = get_messages(service, query=query_html_list)
        if messages:
            for message in messages:
                get_message_content_html(service, message['id'])
            # if "magui.cheung@northatlanticcontainer.com" in query:
            #     for i in range(0, len(messages)):
            #         get_message_content_html(service, messages[i]['id'])

    for query_plain_list in query_plain_lists:
        # Get the messages matching the query
        messages = get_messages(service, query=query_plain_list)
        if messages:
            for message in messages:
                get_message_content_plain(service, message['id'])
            # if "jenny@icc-solution.com" in query:
            #     for i in range(0, len(messages)):
            #         get_message_content_plain(service, messages[i]['id'])

if __name__ == '__main__':
    main()
