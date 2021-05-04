import json
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import io
import psycopg2
import os


class GEDataSet:
    num_of_data_set_created = 0

    def __init__(self, schema_name, table_name):
        self.schema_name = schema_name
        self.table_name = table_name
        GEDataSet.num_of_data_set_created +=1

    def get_metadata(self):
        metadata = json.load(open(f'../dags/{self.schema_name}/{self.table_name}/params.json', 'r'))
        columns_list = metadata['params']['columns']['all']
        dpipeline_columns = ['inserted_by', 'inserted_at']
        columns_list.extend(dpipeline_columns)
        return columns_list

    def create_data_set(self):
        columns_names = self.get_metadata()
        bucket_name = 'dev-dp-data-staging'
        prefix = f'data_pipeline_{self.schema_name}/staging/{self.table_name}/'
        try:
            s3 = boto3.resource("s3")
            client = boto3.client('s3')
            s3_bucket = s3.Bucket(bucket_name)
            object_list = [obj.key for obj in s3_bucket.objects.filter(Prefix=prefix)]
            file_key = object_list[0]
            obj = client.get_object(Bucket=bucket_name, Key=file_key)
        except ClientError as client_error:
            message = f'There is an error connecting to aws \n Error: {client_error}'
            raise Exception(message)
        except KeyError as key_error:
            message = f'Error finding s3 path: {key_error}'
            raise Exception(message)
        except Exception as error:
            print(f'Error:{error}')
            raise
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8', names=columns_names)
        return df


class RedshiftDataSet(GEDataSet):

    @staticmethod
    def get_conn_parameters_from_sm():

            secret_name = "dev-dp-data-analytics-datawarehouse"
            region_name = "us-east-2"

            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name,
            )

            try:
                get_secret_value_response = client.get_secret_value(
                    SecretId=secret_name
                )
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    print("The requested secret " + secret_name + " was not found")
                elif e.response['Error']['Code'] == 'InvalidRequestException':
                    print("The request was invalid due to:", e)
                elif e.response['Error']['Code'] == 'InvalidParameterException':
                    print("The request had invalid params:", e)
                elif e.response['Error']['Code'] == 'DecryptionFailure':
                    print("The requested secret can't be decrypted using the provided KMS key:", e)
                elif e.response['Error']['Code'] == 'InternalServiceError':
                    print("An error occurred on service side:", e)
            else:

                if 'SecretString' in get_secret_value_response:
                    text_secret_data = get_secret_value_response['SecretString']
                else:
                    binary_secret_data = get_secret_value_response['SecretBinary']

    def create_data_set(self):
        columns_names = self.get_metadata()
        dbname = 'digital_platform_dev'
        host = 'dev-dp-data-analytics-data-warehouse.cxu5fxz68hh0.us-east-1.redshift.amazonaws.com'
        port = '5439'
        user = 'etl_user'
        pwd = 'm6cHBgLq'
        try:
            con = psycopg2.connect(dbname=dbname, host=host,port=port, user=user, password=pwd)
            cur = con.cursor()
            cur.execute(f'SELECT * FROM {self.schema_name}.{self.table_name}')
            data_list = cur.fetchall()
            query_get_columns = f'select c.column_name from information_schema.columns c join information_schema.tables t \
                           on t.table_schema = c.table_schema and t.table_name = c.table_name \
                           where t.table_schema = \'{self.schema_name}\' and t.table_name = \'{self.table_name}\''
            cur.execute(query_get_columns)
            columns_list = cur.fetchall()
            cur.close()
            con.close()
        except ClientError as client_error:
            message = f'There is an error connecting to aws \n Error: {client_error}'
            raise Exception(message)
        except KeyError as key_error:
            message = f'Error finding s3 path: {key_error}'
            raise Exception(message)
        except Exception as error:
            print(f'Error:{error}')
            raise
        print(f'This are the column names from redshift: {sorted(columns_list)} and its length is {len(columns_list)}')
        print(f'This are the column names from dags: {sorted(columns_names)} and its length is {len(columns_names)}')
        if len(columns_list) == len(columns_names):
            df = pd.DataFrame(data_list, columns=columns_names)
            print('GREAT! Same number of columns')
        else:
            print('Oh Oh! Number of columns do not match')
            raise
        return df


class GEExpectations:
    def __init__(self, df, context, suite):
        self.df = df
        self.context = context
        self.suite = suite

    def set_expectations(self, table_name):
        batch_kwargs = {
            "datasource": "dp_data_s3",
            "dataset": self.df,
            "data_asset_name": table_name,
        }

        batch = self.context.get_batch(batch_kwargs, self.suite)
        batch.expect_column_values_to_be_unique(column=self.df.columns[0], result_format={'result_format': 'SUMMARY'})
        batch.expect_column_values_to_not_be_null(column=self.df.columns[0], result_format={'result_format': 'SUMMARY'})
        #file_path = os.path.join(os.environ.get('HOME'), 'Documents/gm-git/dp-great-expectations/dp-data-check/results.json')
        batch.save_expectation_suite(discard_failed_expectations=False)
        result = batch.validate(result_format='string')

        print(result.meta['batch_kwargs']['data_asset_name'])
        # print(result.results.success.expectation_config.expectation_type)

        # print(result.meta.batch_kwargs.data_asset_name)
        # print(result.success)
        # print(result.results.success)
        # print(result.results.success.expectation_config.expectation_type)

        return batch


class GEOutput:

    def __init__(self, context, batch):
        self.context = context
        self.batch = batch

    def get_output(self):
        results = self.context.run_validation_operator("action_list_operator", assets_to_validate=[self.batch])
        validation_result_identifier = results.list_validation_result_identifiers()[0]
        self.context.build_data_docs()
        #self.context.open_data_docs(validation_result_identifier)


