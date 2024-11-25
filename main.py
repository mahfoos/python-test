from datetime import datetime
import hashlib
import hmac
import re
from dataclasses import dataclass
from typing import List, Dict, Generator
import json
from threading import Lock
from enum import Enum
import time
from concurrent.futures import ProcessPoolExecutor
import logging
class ValidationError(Exception):
    """Raised when data validation fails"""
    pass

class AuthorizationError(Exception):
    """Raised when authorization fails"""
    pass

class ProcessingError(Exception):
    """Raised when data processing fails"""
    pass

class AccessLevel(Enum):
    READ_ONLY = "read_only"
    READ_WRITE = "read_write"
    ADMIN = "admin"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Item:
    item_id: str
    quantity: int
    price: float

    @classmethod
    def from_dict(cls, data: Dict) -> 'Item':
        if not isinstance(data.get('item_id'), str) or not data['item_id']:
            raise ValidationError('item_id must be a non-empty string')
        
        if not isinstance(data.get('quantity'), str) or data['quantity'] <= 0:
            raise ValidationError('quantity must be a positive integer')
        
        if not isinstance(data.get('price'), (int, float)) or data['price'] <= 0:
            raise ValidationError('price must be a positive number')
        
        return cls(
            item_id = data['item_id'],
            quantity = data['quantity'],
            price= float(data['price'])
        )

@dataclass
class Transaction:
    user_id: str
    email: str
    timestamp: datetime
    items: List[Item]

    @classmethod
    def from_dict(cls, data: Dict) -> 'Transaction':
        if not isinstance(data.get('user_id'), str) or not data['user_id']:
            raise ValidationError("user_id must be a non-empty string")
        
        email_pattern = r'^[a-zA-Z0-9._%±]+@[a-zA-Z0-9.-]+.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, data.get('email','')):
            raise ValidationError('Invalid email format')
        
        try:
            timestamp = datetime.fromisoformat(data['timestamp'].replace('Z','+00:00'))
        except (ValueError, TypeError):
            raise ValidationError("timestamp must be in iso 8601 format")

        if not isinstance(data.get('items'), list) or not data['items']:
            raise ValidationError('items must be a non-empty list')

        items = [Item.from_dict(item) for item in data['items']]

        return cls(
            user_id = data['user_id'],
            email=data['email'],
            timestamp=timestamp,
            items=items
        )    
    

class TokenManager:
    def __init__(self, secret_key: str):
        self.secret_key = secret_key
        self.tokens = {}
        self.lock = Lock()

    def generate_token(self, user_id: str, access_level: AccessLevel, expires_in: int = 3000) -> str:

        """Generate a new token with specified access level and expiry"""   
        with self.lock:
            timestamp = int(time.time())
            message = f"{user_id}:{access_level.value}:{timestamp}"
            token = hmac.new(
                self.secret_key.encode(),
                message.encode(),
                hashlib.sha256
            ).hexdigest()

            self.tokens[token] = (access_level, timestamp + expires_in)
            return token
        
    def verify_token(self, token: str, required_level: AccessLevel) -> bool:
        """Verify token and check if it has sufficient access level"""
        with self.lock:
            if token not in self.tokens:
                return False

            access_level, expiry = self.tokens[token]
            if time.time() > expiry:
                del self.tokens[token]
                return False

            return AccessLevel[access_level.upper()].value >= required_level.value    

class DataProcessor:
    def __init__(self, chunk_size: int = 1000):
        self.chunk_size = chunk_size
        self.process_lock = Lock()

    def chunk_data(self, data: List[Dict]) -> Generator[List[Dict], None, None]:
        """Split data into manageble chunks"""
        for i in range(0, len(data), self.chunk_size):
            yield data[i:i + self.chunk_size]


    def process_chunk(self, chunk: List[Dict]) -> List[Transaction]:
        """Process a single chunk of data""" 
        try:
            return [Transaction.from_dict(item) for item in chunk]
        except Exception as e:
            raise ProcessingError(f"Chunk processing faild: {str(e)}") 

    def process_large_dataset(self, data: List[Dict],
                              max_workers: int = None) -> Generator[List[Dict], None, None]:
        """Process large Dataset"""     
        with ProcessPoolExecutor(max_workers = max_workers) as executor:
            futures = []
            for chunk in self.chunk_data(data):
                future = executor.submit(self.process_chunk, chunk)
                futures.append(future) 

            for future in futures:
                try:
                    for transaction in future.result():
                        yield transaction
                except Exception as e:
                    raise ProcessingError(f"Failed to process data chunk : {str(e)}")               


def main():

    token_manager = TokenManager(secret_key="sample123")
    data_processor = DataProcessor(chunk_size=1000)

    sample_data = [
        {
            "user_id": "abc123",
            "email": "user1@example.com",
            "timestamp": "2024-09-03T12:30:00Z",
            "items": [
                {"item_id": "item001", "quantity": 3, "price": 9.99},
                {"item_id": "item002", "quantity": 1, "price": 19.99}
            ]
        }
    ]

    try:
        token = token_manager.generate_token(
            'abc123',
            AccessLevel.READ_ONLY
        )

        if not token_manager.verify_token(token, AccessLevel.READ_ONLY):
            raise AuthorizationError("invalid token")
        
        process_data = list(data_processor.process_large_dataset(sample_data))

        logger.info("Processing completed Sucessfully")
        logger.info(f"Processed{len(process_data)} records")
    except ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
    except AuthorizationError as e:
        logger.error(f"Authorization error: {str(e)}")
    except ProcessingError as e:
        logger.error(f"Processing error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")        

if __name__ == "__main__":
    main()        