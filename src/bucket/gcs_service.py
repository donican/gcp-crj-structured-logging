from __future__ import annotations
import logging
import pandas as pd
from google.cloud import storage
import io
import mimetypes
import zipfile
from logger.logging import log_error, log_info
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED




class GCSService:
    def __init__(self, bucket_name: str) -> None:
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(__name__)
        self.client = storage.Client()

    def upload_text(self, object_name: str, content:str) -> str:
        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)
            blob.upload_from_string(content, content_type="text/plain")

            gcs_uri = f"gs://{self.bucket_name}/{object_name}"

            log_info(
                self.logger,
                "Text upload completed",
                event="gcs_upload_text_completed",
                bucket_name=self.bucket_name,
                object_name=object_name,
                gcs_uri=gcs_uri,
            )

            return gcs_uri

        except Exception as exc:
            log_error(
                self.logger,
                "Text upload failed",
                event="gcs_upload_text_failed",
                bucket_name=self.bucket_name,
                object_name=object_name,
                error=str(exc),
            )
            raise

    def extract_zip_to_bucket(
            self,
            source_object_name:str,
            destination_bucket_name:str,
            destination_prefix:str=""
    ) ->list[str]:
        extracted_uris: list[str] = []

        try:
            source_bucket = self.client.bucket(self.bucket_name)
            source_blob = source_bucket.blob(source_object_name)
            destination_bucket = self.client.bucket(destination_bucket_name)

            log_info(
                logger=self.logger,
                source_bucket = self.bucket_name,
                source_blob = source_object_name,
                destination_bucket = destination_bucket_name,
                message = "Extract zip to bucket initialized."
            )

            with source_blob.open("rb") as source_file:
                with zipfile.ZipFile(source_file) as zip_file:
                    members = zip_file.infolist()
                    log_info(
                        logger=self.logger,
                        message="Zip file open successfully.",
                        members_count=len(members)
                    )

                    for member in members:
                        log_info(
                            logger=self.logger,
                            message= f"Checking {member.filename}."
                        )
                        if member.is_dir():
                            log_info(
                                logger=self.logger,
                                message = "Member is dir.",
                                
                            )
                            continue
                    
                    content_type, content_encoding = mimetypes.guess_type(member.filename)


                    log_info(
                        logger = self.logger,
                        message=f"Contenty type: {content_type}. Encoding: {content_encoding}"
                        )
                    
                    destination_object_name = f"{destination_prefix}/{member.filename}"
                    destination_blob = destination_bucket.blob(destination_object_name)

                    destination_blob.chunk_size = 8 * 1024 * 1024

                    log_info(
                        logger=self.logger,
                        message = "Extracting zip member",
                        zip_member_name = member.filename,
                        zip_member_size = member.file_size,
                    )

                    # with zip_file.open(member, "r") as extracted_file:
                    #     destination_blob.upload_from_file(
                    #         extracted_file,
                    #         size=member.file_size,
                    #         rewind=False
                    #     )

                    LOG_EVERY = 10 * 1024 * 1024
                    CHUNK = 1 * 1024 * 1024

                    uploaded = 0
                    next_log= LOG_EVERY

                    with zip_file.open(member, "r") as src, destination_blob.open("wb", chunk_size=CHUNK) as dst:
                        while True:
                            data = src.read(CHUNK)
                            if not data:
                                break

                            dst.write(data)
                            uploaded += len(data)
                            
                            if uploaded >= next_log:
                                log_info(
                                    logger=self.logger,
                                    message="Upload progress",
                                    zip_member_name=member.filename,
                                    uploaded_bytes=uploaded,
                                    uploaded_mb=round(uploaded / 1024 / 1024, 2),
                                    total_bytes=member.file_size,
                                )
                                next_log += LOG_EVERY

        except Exception as exc:
            raise
    
    def _write_chunk_to_gcs (self, df, sink_bucket_name, sink_object_name) -> str:
        buffer = io.BytesIO()

        df.to_parquet(
            buffer,
            index=False,
            compression="snappy",
            engine="pyarrow"
        )
        
        buffer.seek(0)

        sink_bucket = self.client.bucket(sink_bucket_name)
        sink_blob_name = f"{sink_object_name}"
        sink_blob = sink_bucket.blob(sink_blob_name)

        sink_blob.upload_from_file(
            buffer,
            content_type="application/octet-stream"
        )

        return f"gs://{sink_bucket_name}/{sink_object_name}"

    def transform_csv_to_parquet(
            self,
            chumk:int= 5 * 100 * 1000,
            source_prefix:str = "2026-02",
            sink_bucket_name:str = "dev-processed-structured-logging",
            sink_prefix:str = "2026-02",
            compression: str = "zip"
    ):
        
        max_workers = 15
        in_flight = set()


        source_bucket = self.client.bucket(self.bucket_name)
        source_blob_name = f"{source_prefix}/Estabelecimentos0.zip"
        source_blob = source_bucket.blob(blob_name=source_blob_name)

        with source_blob.open("rb") as f:
            reader = pd.read_csv(
                f,
                compression=compression,
                chunksize=chumk,
                sep=";",
                dtype=str,
                encoding="latin1"
                )
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:

                for n, df in enumerate(reader):
                    sink_object_name = f"{sink_prefix}/Estabelecimentos0/part-{n:05d}.parquet"
                    
                    future = executor.submit(
                        self._write_chunk_to_gcs,
                        df.copy(),
                        sink_bucket_name,
                        sink_object_name
                    )
                    in_flight.add(future)

                    if len(in_flight) >= max_workers:
                        done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)

                        for f in done:
                            print(f.result())

                for f in in_flight:
                    print(f.result())

            log_info(
                self.logger,
                "Parquet upload completed",
                event="gcs_parquet_upload_completed",
                rows=len(df),
            )

        return
        