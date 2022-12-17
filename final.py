from pathlib import Path
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
import os


def run_sample(directory):    
    conn_str="DefaultEndpointsProtocol=https;AccountName=storageaccountponleb;AccountKey=PvDsGnKvUdY3taoLAow0IQfPXibja0OYvmIuJI8O17XLO+/TwMabNE8IMvNLya+Rgemrk6hYsVw0+AStGE2i5A==;EndpointSuffix=core.windows.net"
    container_name="input"    

    path_remove = str(Path(__file__).parent.absolute())
    directory="/"+directory
    local_path = str(Path(__file__).parent.absolute())+(directory)

    service_client=BlobServiceClient.from_connection_string(conn_str)
    container_client = service_client.get_container_client(container_name)
    print("connected successfully to storage")  
    print("Starting upload")
    for r,d,f in os.walk(local_path):

        if f:
            for file in f:
                file_path_on_azure = os.path.join(r,file).replace(path_remove,"")
                file_path_on_local = os.path.join(r,file)

                blob_client = container_client.get_blob_client(file_path_on_azure)

                with open(file_path_on_local,'rb') as data:
                    blob_client.upload_blob(data)
        else:
            print("else")


if __name__ == '__main__':
    run_sample("stocks_data")
    print("L'upload est complété")