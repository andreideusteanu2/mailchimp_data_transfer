#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  6 15:56:53 2018

@author: developer
"""


def upload_to_gcs(file_path,bucket_name,log_file_path=None,make_blob_public=False,remove_local_file=True):
    
    from connect import Connector
    storage_client = Connector().connect_to_gcs()
    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(file_path.parts[-1])
    blob.upload_from_filename(str(file_path))
    print(str(file_path))
    if make_blob_public:
        blob.make_public()
    
    file_name=file_path.parts[-1]

    if log_file_path is not None:
        with open(str(log_file_path),'w') as f:
            f.write('File {0} uploaded to google cloud storage bucket {1} \n'.format(file_name,bucket_name))
    else:
        print('File {0} uploaded to google cloud storage bucket {1} \n'.format(file_name,bucket_name))
    
    if remove_local_file:
        from os import remove
        remove(str(file_path))
    
    return 'gs://'+bucket_name+'/'+file_name

def transfer_to_gbq(dataset_id,gs_uri,table_id,source_format,autodetect,partitioning_field_name,log_file_path=None,):
    
    from google.cloud import bigquery
    from connect import Connector
    bigquery_client=Connector().connect_to_gbq()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    if autodetect:
        job_config.autodetect = autodetect
        num_rows_before=0
    else:
        table=bigquery_client.get_table(table_ref)
        num_rows_before=table.num_rows
        job_config.schema=table.schema
        if source_format=='CSV':
            job_config.skip_leading_rows=1
    if partitioning_field_name:
        job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partitioning_field_name)  # name of column to use for partitioning)
    
    job_config.source_format = source_format
    uri = gs_uri
    
    load_job = bigquery_client.load_table_from_uri(
        uri,
        dataset_ref.table(table_id),
        location='EU',  # Location must match that of the destination dataset.
        job_config=job_config)  # API request
    
    if log_file_path is not None:
        with open(str(log_file_path),'w') as f:
            f.write('Starting job {} .'.format(load_job.job_id))
        
        load_job.result()  # Waits for table load to complete.
        with open(str(log_file_path),'w') as f:
            f.write('Job finished. ')
        
        destination_table = bigquery_client.get_table(dataset_ref.table(table_id))
        num_rows_after=destination_table.num_rows
        with open(str(log_file_path),'w') as f:
            f.write('Loaded {} rows.\n'.format(num_rows_after-num_rows_before))
    else:
        
        print('Starting job {} .'.format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.
        print('Job finished. ')
        destination_table = bigquery_client.get_table(dataset_ref.table(table_id))
        num_rows_after=destination_table.num_rows
        print('Loaded {} rows.\n'.format(num_rows_after-num_rows_before))
        
def main(file_path,bucket_name,dataset_id,table_id,log_file_path=None
         ,make_blob_public=False,source_format='NEWLINE_DELIMITED_JSON'
         ,remove_local_file=True,autodetect=False
         ,partitioning_field_name=None):
    gs_uri=upload_to_gcs(file_path,bucket_name,log_file_path,make_blob_public,remove_local_file)
    transfer_to_gbq(dataset_id,gs_uri,table_id,source_format,autodetect,partitioning_field_name,log_file_path)