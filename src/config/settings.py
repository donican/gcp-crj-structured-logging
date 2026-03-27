import os
import logging
from logger.logging import log_error, log_info
from dotenv import load_dotenv

class Settings:
    def __init__(self) -> None:

        load_dotenv()
        self.bucket_name = os.getenv("BUCKET_NAME","").strip()
        self.env = os.getenv("ENV", "DEV").strip()
        self.version = os.getenv("VERSION", "1.0.0").strip()

        self.logger = logging.getLogger(__name__)

    def validate(self) -> None:

        log_info(
            logger=self.logger,
            message="Settings validation started",
            event="settings_validation_started"
        )

        if not self.bucket_name:
            
            log_error(
                self.logger, 
                "BUCKET_NAME environment variable is required.",
                event="settings_validation_failed",
                missing_variable="BUCKET_NAME",
            )

            raise ValueError("BUCKET_NAME environment variable is required.")

        log_info(
            logger=self.logger,
            message="Settings validation succeeded",
            event="settings_validation_succeeded",
        )