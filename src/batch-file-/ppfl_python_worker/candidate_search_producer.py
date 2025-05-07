import json
import os
from urllib.parse import urljoin

import pika
import requests
from dotenv import load_dotenv

load_dotenv()


def get_keycloak_token():
    """
    Obtain an access token from Keycloak using username/password (Resource Owner Password Flow).
    """
    token_url = os.environ.get("KEYCLOAK_TOKEN_URL")
    client_id = os.environ.get("KEYCLOAK_CLIENT_ID")
    username = os.environ.get("ADMIN_USERNAME")
    password = os.environ.get("ADMIN_PASSWORD")

    if not all([token_url, client_id, username, password]):
        raise ValueError("Missing required Keycloak environment variables.")

    data = {
        "grant_type": "password",
        "client_id": client_id,
        "username": username,
        "password": password,
    }

    resp = requests.post(token_url, data=data)
    resp.raise_for_status()
    token_data = resp.json()
    return token_data["access_token"]


def get_candidate_search_status(organization_id, experiment_id, token):
    """
    Call the GET /api/fedml-experiments/{organizationId}/{experimentId}/candidate_search_status endpoint
    using the Bearer token.
    """
    base_url = os.environ.get("API_BASE_URL", "http://localhost")
    endpoint = f"/api/fedml-experiments/{organization_id}/{experiment_id}/candidate_search_status"
    url = urljoin(base_url, endpoint)

    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def publish_to_rabbitmq(message):
    """
    Publish the message (Python dict) to the specified RabbitMQ queue.
    The queue name, host, and port are taken from environment variables.
    """
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_port = int(os.environ.get("RABBITMQ_PORT", "5672"))
    queue_name = os.environ.get("RABBITMQ_QUEUE", "queue_A")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port)
    )
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=True)

    body = json.dumps(message)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )

    print(
        f"[Producer] Published to {queue_name} on {rabbitmq_host}:{rabbitmq_port} => {body}"
    )
    connection.close()


def main():
    """
    Main flow:
      1. Get Keycloak token.
      2. GET candidate search status from the API.
      3. Publish the API response to RabbitMQ (queue_A by default).
    """
    organization_id = os.environ.get("ORGANIZATION_ID")
    experiment_id = os.environ.get("EXPERIMENT_ID")

    if not organization_id or not experiment_id:
        raise ValueError(
            "Missing ORGANIZATION_ID or EXPERIMENT_ID in environment variables."
        )

    token = get_keycloak_token()
    status_response = get_candidate_search_status(organization_id, experiment_id, token)
    publish_to_rabbitmq(status_response)


if __name__ == "__main__":
    main()
