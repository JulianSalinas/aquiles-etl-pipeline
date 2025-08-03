"""
Azure Storage operations module for blob handling.
"""
import logging
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


def get_blob_service_client(storage_account_name):
    """Get Azure Blob Service Client using Azure Default Credential."""
    try:
        account_url = f"https://{storage_account_name}.blob.core.windows.net"

        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        
        logging.info("Successfully created Blob Service Client using Azure Default Credential")
        return blob_service_client
        
    except Exception as e:
        raise ValueError("Azure Default Credential failed for blob storage")


def read_blob_content(blob_service_client, container_name, blob_name):
    """Read blob content from Azure Storage."""
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Download blob content
        blob_data = blob_client.download_blob()
        content = blob_data.readall()
        
        logging.info(f"Successfully read blob: {blob_name} from container: {container_name}")
        return content
        
    except Exception as e:
        logging.error(f"Error reading blob {blob_name} from container {container_name}: {str(e)}")
        raise


def get_blob_properties(blob_service_client, container_name, blob_name):
    """Get blob properties and metadata."""
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        properties = blob_client.get_blob_properties()
        
        return {
            "name": blob_name,
            "container": container_name,
            "size": properties.size,
            "last_modified": properties.last_modified,
            "content_type": properties.content_settings.content_type,
            "etag": properties.etag
        }
        
    except Exception as e:
        logging.error(f"Error getting blob properties for {blob_name}: {str(e)}")
        raise


def list_blobs_in_container(blob_service_client, container_name, name_starts_with=None):
    """List all blobs in a container."""
    try:
        container_client = blob_service_client.get_container_client(container_name)
        
        blobs = container_client.list_blobs(name_starts_with=name_starts_with)
        
        blob_list = []
        for blob in blobs:
            blob_list.append({
                "name": blob.name,
                "size": blob.size,
                "last_modified": blob.last_modified
            })
        
        logging.info(f"Found {len(blob_list)} blobs in container {container_name}")
        return blob_list
        
    except Exception as e:
        logging.error(f"Error listing blobs in container {container_name}: {str(e)}")
        raise
