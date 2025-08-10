"""
Azure Storage operations module for blob handling.
"""
import logging
from dataclasses import dataclass
from io import StringIO

from azure.core.paging import ItemPaged
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (BlobClient, BlobProperties, BlobServiceClient,
                                ContainerClient, StorageStreamDownloader)


@dataclass
class BlobInfo:
    name: str
    container: str
    size: int
    last_modified: str
    content_type: str | None
    etag: str

def get_blob_service_client(storage_account_name: str) -> BlobServiceClient:
    """Get Azure Blob Service Client using Azure Default Credential."""
    try:
        account_url: str = f"https://{storage_account_name}.blob.core.windows.net"

        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        
        logging.info("Successfully created Blob Service Client using Azure Default Credential")
        return blob_service_client

    except Exception:
        raise ValueError("Azure Default Credential failed for blob storage")


def read_blob_content(blob_service_client: BlobServiceClient, container_name: str, blob_name: str) -> bytes:
    """Read blob content from Azure Storage."""
    try:
        blob_client: BlobClient = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Download blob content
        blob_data: StorageStreamDownloader[bytes] = blob_client.download_blob()
        content = blob_data.readall()
        
        logging.info(f"Successfully read blob: {blob_name} from container: {container_name}")
        return content
        
    except Exception as e:
        logging.error(f"Error reading blob {blob_name} from container {container_name}: {str(e)}")
        raise


def get_blob_properties(blob_service_client: BlobServiceClient, container_name: str, blob_name: str) -> BlobInfo:
    """Get blob properties and metadata."""
    try:
        blob_client: BlobClient = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        properties: BlobProperties = blob_client.get_blob_properties()
    
        return BlobInfo(
            name=blob_name,
            container=container_name,
            size=properties.size,
            last_modified=str(properties.last_modified),
            content_type=properties.content_settings.content_type,
            etag=properties.etag
        )
        
    except Exception as e:
        logging.error(f"Error getting blob properties for {blob_name}: {str(e)}")
        raise


def list_blobs_in_container(blob_service_client: BlobServiceClient, container_name: str, name_starts_with: str | None = None) -> list[BlobInfo]:
    """List all blobs in a container."""
    try:
        container_client: ContainerClient = blob_service_client.get_container_client(container_name)
        
        blobs: ItemPaged[BlobProperties] = container_client.list_blobs(name_starts_with=name_starts_with)

        return [BlobInfo(
            name=blob.name,
            container=container_name,
            size=blob.size,
            last_modified=str(blob.last_modified),
            content_type=blob.content_settings.content_type,
            etag=blob.etag
        ) for blob in blobs]

    except Exception as e:
        logging.error(f"Error listing blobs in container {container_name}: {str(e)}")
        raise


def upload_blob_content(blob_service_client: BlobServiceClient, container_name: str, blob_name: str, content: bytes | str | StringIO, content_type: str = "text/csv"):
    """Upload content to Azure Storage as a blob."""
    try:
        blob_client: BlobClient = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        blob_client.upload_blob(content, overwrite=True, content_settings={'content_type': content_type})
        
        logging.info(f"Successfully uploaded blob: {blob_name} to container: {container_name}")

    except Exception as e:
        logging.error(f"Error uploading blob {blob_name} to container {container_name}: {str(e)}")
        raise
