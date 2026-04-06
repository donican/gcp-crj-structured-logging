import logging
import time
import uuid

from logger.logging import setup_logging, log_info, log_error
from logger.context import LogContext
from config.settings import Settings
from bucket.gcs_service import GCSService


class App:
    def __init__(self) -> None:
        setup_logging()

        self.logger = logging.getLogger(__name__)        
        self.settings = Settings()
        self.storage = GCSService(self.settings.bucket_name)
        self.execution_id = str(uuid.uuid4())


    def run(self) -> None:
        start_time = time.perf_counter()

        with LogContext(component="main_app", env=self.settings.env, execution_id=self.execution_id):

            log_info(
                logger=self.logger,
                message="Job started",
                event="job_started",
            )

            try:
                self.settings.validate()

                #self.storage.upload_text("teste.txt", "My content")
                
                # self.storage.extract_zip_to_bucket(
                #     source_object_name="2026-02/Empresas3.zip",
                #     destination_bucket_name="dev-processed-structured-logging"
                # )
                
                files = self.storage.list_zip_files(prefix="2026-02/")
                task_index = self.settings.cloud_run_task_index


                file_to_process = files[task_index]
                log_info(
                    logger=self.logger,
                    message=f"Processing task {task_index}. Reading file {file_to_process}",
                    event="task_processing"
                )


                #file_to_process = files[11]
                print(file_to_process)
                self.storage.transform_csv_to_parquet(source_blob_name=file_to_process)

                duration_ms = round((time.perf_counter() - start_time) * 1000)

                log_info(
                    logger=self.logger,
                    message="Job completed successfully",
                    event="job_completed",
                    duration_ms=duration_ms
                )

            except Exception as exc:
                duration_ms = round((time.perf_counter() - start_time) * 1000)

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