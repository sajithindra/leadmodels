import jwt
import secrets

def create_token():
    key = secrets.token_urlsafe(16)
