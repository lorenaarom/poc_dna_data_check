from botocore.exceptions import ClientError


class GEDataSet:
    def __init__(self, schema_name, table_name):
        self.schema_name = schema_name
        self.table_name = table_name

    def get_metadata(self):
        metadata = json.load(open(f'../dags/{self.schema_name}/{self.table_name}/params.json', 'r'))
        columns_list = metadata['params']['columns']['all']
        dpipeline_columns = ['inserted_by', 'inserted_at', 'topic']
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
