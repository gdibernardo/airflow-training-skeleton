from tempfile import NamedTemporaryFile
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action
    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """
    template_fields = ("bucket", "endpoint", "filename", "parameters")
    template_ext = ()
    ui_color = '#f4a460'
    @apply_defaults
    def __init__(self,
                 http_conn_id,
                 endpoint,
                 bucket,
                 filename,
                 google_cloud_storage_conn_id="google_cloud_default",
                 delegate_to=None,
                 parameters=None,
                 *args, **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.bucket = bucket
        self.filename = filename
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.http_connection_id = http_conn_id
        self.delegate_to = delegate_to
        self.parameters = parameters

    def execute(self, context):
        http_response = self._http_get()
        file_handle = self._write_local_data_files(http_response)
        file_handle.flush()
        self._upload_to_gcs(file_handle)

    def _http_get(self):
        http = HttpHook('GET', http_conn_id=self.http_connection_id)

        self.log.info("Calling HTTP method")

        return http.run(self.endpoint).text

    def _upload_to_gcs(self, file_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google Cloud Storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to,
        )

        hook.upload(self.bucket, self.filename, file_to_upload.name, "application/json")

    def _write_local_data_files(self, json_content):
        """
        Takes a cursor, and writes results to a local file.

        :return: A file name to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        tmp_file_handle = NamedTemporaryFile(delete=True)

        tmp_file_handle.write(json_content)
        # Append newline to make dumps BigQuery compatible.
        tmp_file_handle.write(b"\n")
        return tmp_file_handle
