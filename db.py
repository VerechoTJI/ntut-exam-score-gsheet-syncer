import os
import requests
from dotenv import load_dotenv

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
        response = requests.get(f"{api_url}/admin/scores", headers=headers)
        response.raise_for_status()
        data = response.json()
        
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
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from backend API: {e}")
        return [], []
