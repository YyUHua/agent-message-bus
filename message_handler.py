import sys
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to path for imports
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.append(_PROJECT_ROOT)

from message_processor import process_incoming_message

def handle_incoming_message(source, message_id, content):
    """
    Handle an incoming message with deduplication
    """
    logger.info(f"Handling message from {source}")
    is_processed = process_incoming_message(source, message_id, content)
    if is_processed:
        logger.info("Message processed successfully")
    else:
        logger.info("Message was a duplicate and skipped")
    return is_processed