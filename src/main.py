import logging
from logger.logging import setup_logging, log_info
from config.settings import Settings

class App:
    def __init__(self) -> None:
        setup_logging()
        self.logger = logging.getLogger(__name__)        
        self.settings = Settings()

        log_info(
            logger=self.logger,
            message="Application initialized."
        )

    def run(self) -> None:
        self.settings.validate()
    
if __name__ == "__main__":
    app = App()
    app.run()