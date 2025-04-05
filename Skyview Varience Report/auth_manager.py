import boto3
from botocore.exceptions import ClientError
import logging
import jwt
import time
import os
import json

logger = logging.getLogger(__name__)

TOKEN_CACHE_FILE = '.token_cache'

def load_cached_token():
    try:
        if os.path.exists(TOKEN_CACHE_FILE):
            with open(TOKEN_CACHE_FILE, 'r') as f:
                cache = json.load(f)
                token = cache.get('token')
                if token:
                    try:
                        # Decode token to check expiry
                        decoded = jwt.decode(token, options={"verify_signature": False})
                        if decoded['exp'] > time.time():
                            return token
                    except:
                        pass
    except Exception as e:
        logger.error(f"Error loading cached token: {e}")
    return None

def save_token_to_cache(token):
    try:
        with open(TOKEN_CACHE_FILE, 'w') as f:
            json.dump({'token': token}, f)
    except Exception as e:
        logger.error(f"Error saving token to cache: {e}")

def handle_auth_flow(username, password, debugger=None):
    max_attempts = 3
    
    # First try to use cached token
    cached_token = load_cached_token()
    if cached_token:
        return cached_token
    
    try:
        client = boto3.client('cognito-idp', region_name='us-east-1')
        response = client.initiate_auth(
            AuthFlow='USER_PASSWORD_AUTH',
            AuthParameters={
                'USERNAME': username,
                'PASSWORD': password
            },
            ClientId='6fk3ot5ut181jt7r2pdp9h6m5q'
        )
        
        if 'ChallengeName' in response:
            current_attempt = 0
            while current_attempt < max_attempts:
                try:
                    mfa_code = input("Enter MFA code: ")
                    challenge_response = client.respond_to_auth_challenge(
                        ClientId='6fk3ot5ut181jt7r2pdp9h6m5q',
                        ChallengeName=response['ChallengeName'],
                        Session=response['Session'],
                        ChallengeResponses={
                            'USERNAME': username,
                            'SOFTWARE_TOKEN_MFA_CODE': mfa_code
                        }
                    )
                    token = challenge_response['AuthenticationResult']['IdToken']
                    save_token_to_cache(token)
                    return token
                except ClientError as e:
                    current_attempt += 1
                    if current_attempt >= max_attempts:
                        raise e
                    logger.error(f"Invalid MFA code. {max_attempts - current_attempt} attempts remaining.")
                    continue
        
        token = response['AuthenticationResult']['IdToken']
        save_token_to_cache(token)
        return token
                
    except Exception as e:
        if debugger:
            debugger.log_error("Authentication", e)
        logger.error(f"Authentication failed: {str(e)}")
        return None

def get_auth_token(username, password, debugger=None):
    return handle_auth_flow(username, password, debugger)