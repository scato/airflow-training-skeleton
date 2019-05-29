from tempfile import NamedTemporaryFile

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HttpToGoogleCloudStorageOperator(BaseOperator):
    template_fields = ('endpoint', 'filename')

    @apply_defaults
    def __init__(self,
                 endpoint,
                 bucket,
                 filename,
                 task_id,
                 method='POST',
                 data=None,
                 headers=None,
                 extra_options=None,
                 http_conn_id='http_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 **kwargs):
        super().__init__(task_id=task_id, **kwargs)

        self.endpoint = endpoint
        self.bucket = bucket
        self.filename = filename
        self.method = method
        self.data = data
        self.headers = headers
        self.extra_options = extra_options
        self.http_conn_id = http_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to,
        )

        tmp_file_handle = NamedTemporaryFile(delete=True)
        tmp_file_handle.write(response.content)
        tmp_file_handle.flush()

        hook.upload(self.bucket, self.filename, tmp_file_handle.name, "application/json")

        tmp_file_handle.close()
