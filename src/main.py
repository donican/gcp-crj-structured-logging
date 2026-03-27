import logging
import time
import uuid

from logger.logging import setup_logging, log_info, log_error
from logger.context import LogContext
from config.settings import Settings


class App:
    def __init__(self) -> None:
        setup_logging()

        self.logger = logging.getLogger(__name__)        
        self.settings = Settings()
        self.execution_id = str(uuid.uuid4())


    def run(self) -> None:
        start_time = time.time()

        with LogContext(component="main_app", env=self.settings.env, execution_id=self.execution_id):

            log_info(
                logger=self.logger,
                message="Job started",
                event="job_started",
            )

            try:
                self.settings.validate()

                # do something

                duration_ms = int((time.time() - start_time) * 1000)

                log_info(
                    logger=self.logger,
                    message="Job completed successfully",
                    event="job_completed",
                    duration_ms=duration_ms
                )

            except Exception as exc:
                duration_ms = int((time.time() - start_time) * 1000)

                log_error(
                    logger=self.logger,
                    message="Job failed",
                    event="job_failed",
                    duration_ms=duration_ms,
                    error=str(exc)
                )
                raise
    
if __name__ == "__main__":
    app = App()
    app.run()