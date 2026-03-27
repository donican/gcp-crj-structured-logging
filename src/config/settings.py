import os
import logging
from logger.logging import log_error
from dotenv import load_dotenv

class Settings:
    def __init__(self) -> None:
        self.bucket_name = os.getenv("BUCKET_NAME","").strip()

        self.logger = logging.getLogger(__name__)

    def validate(self) -> None:
        if not self.bucket_name:
            
            log_error(
                self.logger, 
                "BUCKET_NAME environment variable is required.",
                event="settings_validation_failed",
                missing_variable="BUCKET_NAME",
            )

            raise ValueError("BUCKET_NAME environment variable is required.")