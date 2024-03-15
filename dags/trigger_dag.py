import requests
from streamlit import session_state as state

def trigger_dag(dag_id, product_name):
    airflow_api_url = "http://localhost:8080/api/v1/dags/{dag_id}/dagRuns".format(dag_id=dag_id)
    api_auth_token = "your_airflow_api_token"
    headers = {
        "Authorization": "Bearer {token}".format(token=api_auth_token),
        "Content-Type": "application/json"
    }
    payload = {
        "conf": {
            "product_name": product_name
        }
    }
    
    response = requests.post(airflow_api_url, json=payload, headers=headers)
    
    return response
    