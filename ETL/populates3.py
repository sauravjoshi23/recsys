import re
import boto3
import pandas as pd
from io import StringIO

class AWSS3(object):

    def __init__(
            self,
            bucket
    ):
        self.bucket = bucket
        self.client = boto3.client("s3") # user credentials set in the environment
        
    def ingest_files(self, Response=None, Key=None):
        try:
            response=self.client.put_object(
                ACL="private", Body=Response, Bucket=self.bucket, Key=Key
            )
            return True
        except Exception as e:
            raise Exception("Failed to upload records. Error: {}".format(e))
        
    def item_exists(self, Key):
        try:
            response=self.client.get_object(
                Bucket=self.bucket, Key=Key
            )
            return True
        except Exception as e:
            return False
    
    def get_item(self, Key):
        try:
            response=self.client.get_object(
                Bucket=self.bucket, Key=Key
            )
            return response["Body"].read()
        except Exception as e:
            print("Error: {}".format(e))
            return False
        
    def delete_item(self, Key):
        response=self.client.delete_object(Bucket=self.bucket, Key=Key)
        return response
    
    def get_all_keys(self, Prefix=""):
        try:
            s3=boto3.resoure('s3')
            b=s3.Bucket(self.bucket)
            tmp=[]
            for obj in b.objects.filter(Prefix=Prefix):
                tmp.append(obj.key)
            return True
        except Exception as e:
            print("Error: {}".format(e))
            return False

    def print_tree(self):
        keys=self.get_all_keys()
        for k in keys:
            print(k)
        return None
    
    def find_one_similar_key(self, searchTerm=""):
        keys=self.get_all_keys()
        return [k for k in keys if re.search(searchTerm, k)]
    
    def __repr__(self):
        return "AWS S3 Helper Class"

def main():

    df = pd.read_csv('../data/ratings.csv', index_col=0)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_obj = AWSS3(
        bucket="recsys-aws"
    )
    key = "raw_data/ratings.csv"
    s3_obj.ingest_files(
        Key=key, Response=csv_buffer.getvalue()
    )

if __name__ == "__main__":
    main()

