import azure.functions as func
import logging

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="products-dev", connection="provider24_STORAGE") 
def provider24_elt_blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob" f"Name: {myblob.name}" f"Blob Size: {myblob.length} bytes")

# This example uses SDK types to directly access the underlying BlobClient object provided by the Blob storage trigger.
# To use, uncomment the section below and add azurefunctions-extensions-bindings-blob to your requirements.txt file
# import azurefunctions.extensions.bindings.blob as blob
# @app.blob_trigger(arg_name="client", path="provider24-dev",
#                   connection="provider24_STORAGE")
# def provider24_elt_blob_trigger(client: blob.BlobClient):
#     logging.info(
#         f"Python blob trigger function processed blob \n"
#         f"Properties: {client.get_blob_properties()}\n"
#         f"Blob content head: {client.download_blob().read(size=1)}"
#     )
