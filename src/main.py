import logging
from logger.logging import setup_logging, log_info
from logger.context import LogContext
from config.settings import Settings

class App:
    def __init__(self) -> None:
        setup_logging()
        self.logger = logging.getLogger(__name__)        
        self.settings = Settings()

        with LogContext(component="main_app", env="production"):
            log_info(
                logger=self.logger,
                message="Application initialized.",
                version="1.0.4" # Exemplo de field extra manual
            )

    def run(self) -> None:
        self.settings.validate()
    
if __name__ == "__main__":
    app = App()
    app.run()