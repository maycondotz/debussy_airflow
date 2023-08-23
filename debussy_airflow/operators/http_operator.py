from airflow.models.baseoperator import BaseOperator
from debussy_airflow.hooks.http_hook import HttpHook


class HTTPOperator(BaseOperator):
    template_fields = ("endpoint","headers","data")

    def __init__(
        self,
        http_hook: HttpHook,
        endpoint,
        headers=None,
        data=None,
        extra_options=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.http_hook = http_hook
        self.endpoint = endpoint
        self.data = data
        self.extra_options = extra_options
        self.headers = headers

    def execute(self, context):
        self.log.info("Calling HTTP method")
        self.log.info(f"headers: {self.headers}")
        self.log.info(f"extra_options: {self.extra_options}")
        response = self.http_hook.run(
            self.endpoint, self.data, self.headers, self.extra_options
        )
        self.log.info(f"response: {response}")
        # returning the data on airflow will push it to xcom
        if self.do_xcom_push:
            self.log.info(f"response TXT: {response.text}")
            return response.text
