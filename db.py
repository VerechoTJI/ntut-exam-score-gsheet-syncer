import os
import requests
from urllib3.util import Timeout
from dotenv import load_dotenv
from retry import run, OperationFailed

def fetch_scoreboard_rows(limit=None):
    load_dotenv()
    
    api_url = os.getenv("BACKEND_API_URL")
    admin_token = os.getenv("ADMIN_TOKEN")
    
    if not api_url or not admin_token:
        print("Error: BACKEND_API_URL or ADMIN_TOKEN not set in .env")
        return [], []
        
    headers = {
        "Authorization": f"Bearer {admin_token}"
    }
    
    try:
        # Fetch data from the API
        def fetch(remaining):
            response = requests.get(f"{api_url.rstrip('/')}/admin/scores", headers=headers, timeout=Timeout(total=remaining))
            response.raise_for_status()
            return response.json()
        data = run('Fetch backend scores', fetch)
        
        if not data:
            return [], []
            
        # The data is expected to be a list of dicts (ScoreBoard model)
        # Apply limit if specified
        if limit:
            try:
                limit_int = int(limit)
                data = data[:limit_int]
            except ValueError:
                pass
                
        # Extract selected columns from the keys of the first item
        selected_cols = list(data[0].keys())
        
        return data, selected_cols
        
    except OperationFailed as e:
        print(f"Error fetching data from backend API: {e}")
        return [], []
