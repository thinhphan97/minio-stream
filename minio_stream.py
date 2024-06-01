from minio import Minio
from minio.commonconfig import Tags
from minio.helpers import (DictType, ObjectWriteResult,
                           check_bucket_name, check_non_empty_string,
                           check_sse, genheaders, MAX_PART_SIZE, MIN_PART_SIZE)
from minio.datatypes import Part
from minio.retention import Retention
from minio.sse import Sse, SseCustomerKey
from typing import cast


class MinioStream(Minio):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def open(self, 
            bucket_name: str, 
            object_name: str,
            content_type: str = "application/octet-stream",
            metadata: DictType | None = None,
            sse: Sse | None = None,
            tags: Tags | None = None,
            retention: Retention | None = None,
            legal_hold: bool = False):

        check_bucket_name(bucket_name, s3_check=self._base_url.is_aws_host)
        check_non_empty_string(object_name)
        check_sse(sse)
        if tags is not None and not isinstance(tags, Tags):
            raise ValueError("tags must be Tags type")
        if retention is not None and not isinstance(retention, Retention):
            raise ValueError("retention must be Retention type")

        headers = genheaders(metadata, sse, tags, retention, legal_hold)
        headers["Content-Type"] = content_type or "application/octet-stream"
        self.sse = sse
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.part_number = 1
        self.parts: list[Part] = []
    
        try:
            self.upload_id = self._create_multipart_upload(
                            self.bucket_name, self.object_name, headers,
                        )
        except Exception as exc:
            if self.upload_id:
                self._abort_multipart_upload(
                    bucket_name, self.object_name, self.upload_id,
                )
            raise exc
        
        return self
    
    def write(self, data: bytes)-> int:
        
        try:
            if len(data) > MAX_PART_SIZE:
                raise IOError(
                            f"data size is too large;"
                            f"expected: {MAX_PART_SIZE}, "
                            f"got: {len(data)} bytes"
                            )
            args = (
                    self.bucket_name,
                    self.object_name,
                    data,
                    (
                        cast(DictType, self.sse.headers())
                        if isinstance(self.sse, SseCustomerKey) else None
                    ),
                    self.upload_id,
                    self.part_number,
                    )
            etag = self._upload_part(*args)
            self.parts.append(Part(self.part_number, etag))
            self.part_number += 1

        except Exception as exc:
            if self.upload_id:
                self._abort_multipart_upload(
                    self.bucket_name, self.object_name, self.upload_id,
                )
            raise exc
    
    def close(self)->ObjectWriteResult:
        
        try:
            upload_result = self._complete_multipart_upload(
                    self.bucket_name, self.object_name, cast(str, self.upload_id), self.parts,
                )
            
            self.bucket_name = None
            self.object_name = None
            self.upload_id = None
            self.parts: list[Part] = []
            self.part_number = 1
            
            return ObjectWriteResult(
                    cast(str, upload_result.bucket_name),
                    cast(str, upload_result.object_name),
                    upload_result.version_id,
                    upload_result.etag,
                    upload_result.http_headers,
                    location=upload_result.location,
                )
        except Exception as exc:
            if self.upload_id:
                self._abort_multipart_upload(
                    self.bucket_name, self.object_name, self.upload_id,
                )
            raise exc
    
    def __enter__(self):
        return self
    
    def __exit__(self, exception_type, exception_value, exception_traceback)->ObjectWriteResult:
        
        return self.close()