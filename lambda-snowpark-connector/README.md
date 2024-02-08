# Connecting Snowpark to AWS Lambda locally using LocalStack

This example demonstrates how to connect Snowpark to AWS Lambda locally using LocalStack. Snowpark allows you to query, process, and transform data in a variety of ways using Snowpark Python. In this example, we create a Lambda function that uses Snowpark to:

- Establish a connection to a local Snowflake database provided by LocalStack.
- Create a cursor object and execute a simple query to create a table.
- Insert rows and execute an insert query with `executemany` and a query to select data from the table.
- Fetch the results and execute a query to get the current timestamp.

## Prerequisites

- Python 3.10 installed locally
- [LocalStack](https://localstack.cloud/) with `LOCALSTACK_AUTH_TOKEN` environment variable set
- [LocalStack Snowflake extension](https://discuss.localstack.cloud/t/introducing-the-localstack-snowflake-extension-experimental/665)

## Instructions

### Install the dependencies

Run the following command to install the dependencies:

```bash
make install
```

### Start the LocalStack container

Start LocalStack after installing the Snowflake extension:

```bash
make start-localstack
```

### Create the Lambda function

You can create the Lambda function using the following command:

```bash
make zip
make create-function
```

### Invoke the Lambda function

Invoke the Lambda function using the following command:

```bash
make local-invoke
```

Open the `output.txt` file to see the results of the Lambda function.

```text
{"statusCode": 200, "body": "Successfully connected to Snowflake and inserted rows!"}
```

In the LocalStack logs (with `DEBUG=1`), you can see the Snowflake queries executed by the Lambda function.

```bash
2024-02-07T17:33:36.763 DEBUG --- [   asgi_gw_3] l.s.l.i.version_manager    : [localstack-snowflake-lambda-example-b0813b21-ad5f-4ec7-8fb4-53147df9695e] Total # of rows: 3
2024-02-07T17:33:36.763 DEBUG --- [   asgi_gw_3] l.s.l.i.version_manager    : [localstack-snowflake-lambda-example-b0813b21-ad5f-4ec7-8fb4-53147df9695e] Row-1 => ('John', 'SQL')
2024-02-07T17:33:36.763 DEBUG --- [   asgi_gw_3] l.s.l.i.version_manager    : [localstack-snowflake-lambda-example-b0813b21-ad5f-4ec7-8fb4-53147df9695e] Row-2 => ('Alex', 'Java')
2024-02-07T17:33:36.771 DEBUG --- [   asgi_gw_3] l.s.l.i.version_manager    : [localstack-snowflake-lambda-example-b0813b21-ad5f-4ec7-8fb4-53147df9695e] Current timestamp from Snowflake: 2024-02-07T17:33:36
```

## License

This code is licensed under the Apache 2.0 License.
