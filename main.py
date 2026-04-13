import os
import psycopg2
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


def main():

    # Load the .env file
    load_dotenv()

    # 1. Setup Google Sheets Authentication
    # Path to the JSON file you downloaded from Google Cloud
    SERVICE_ACCOUNT_FILE = "credentials.json"
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)

    # 2. Connect to the Google Sheet
    SHEET_URL = os.getenv("SHEET_URL")
    spreadsheet = client.open_by_url(SHEET_URL)
    worksheet = spreadsheet.get_worksheet(0)  # Selects the first tab

    # 3. Connect to PostgreSQL using .env variables
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        cur = conn.cursor()

        # 4. Fetch Data
        cur.execute("SELECT * FROM score_boards;")
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        # 5. Prepare and Upload Data
        data_to_write = [colnames]
        for row in rows:
            data_to_write.append(
                [str(item) if item is not None else "" for item in row]
            )

        worksheet.clear()
        worksheet.update("A1", data_to_write)
        print("Update successful!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()


if __name__ == "__main__":
    main()
