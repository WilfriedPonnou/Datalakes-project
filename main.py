import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
import glob
import os
import io
from io import BytesIO
CONNECTION_STRING= os.get_env(CONNECTION_STRING)
CONTAINERNAME="input"
OUTPUTCONTAINERNNAME="output"


def main():

  blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
  container_client = blob_service_client.get_container_client(CONTAINERNAME)

  liste=container_client.list_blobs(prefix="stocks_data")

  csv_list = list()
  for blob in liste:
    csv_list.append(blob.name)
  df_l =list()
  csv_list=[i for i in csv_list if i.endswith(".csv")]

  for csv_path in csv_list:
      blob_client = container_client.get_blob_client(csv_path)
      with BytesIO() as input_blob:
          blob_client.download_blob().download_to_stream(input_blob)
          input_blob.seek(0)
          df = pd.read_csv(input_blob)
          df_l.append(df)
  
  df = pd.concat(df_l,ignore_index=True)
  output_container = blob_service_client.get_container_client(OUTPUTCONTAINERNNAME)
  output = df.to_csv(encoding="utf-8",index=False)
  df.to_csv('test.csv',encoding="utf-8",index=False)
  blob_client = output_container.get_blob_client("merged.csv")
  blob_client.upload_blob(output,overwrite=True)


if __name__ == '__main__':
  main()
  print("**Le merge est complété**")
