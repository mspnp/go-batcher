# Usage - Code sample

This code sample demonstrates the usage of the `go-batcher`.

## Prerequisites

- [Go v1.25+](https://golang.org/)
- [Visual Studio Code](https://code.visualstudio.com/)
- [Azure Subscription](https://azure.microsoft.com/en-us/free/)

## Guide

1. Clone this repo [https://github.com/Azure-Samples/go-batcher](https://github.com/Azure-Samples/go-batcher)
2. Open the `/sample` directory in Visual Studio Code
3. `sample/go.mod` already points at the sibling `v2` module via a relative `replace` directive, so no local path edits are needed when working inside a clone of this repo.

4. Create an [Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-create?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&tabs=azure-cli) and a container. This will be the used for the SharedResource rate limiter. Using [Azure CLI](https://docs.microsoft.com/en-gb/cli/azure/install-azure-cli) you can use the following commands:

    ```bash
    # Set your variables
    SUBSCRIPTIONID="insert-azure-subscription-id"
    RESOURCEGROUP="insert-resource-group-name"
    LOCATION="insert-location" #e.g., westus
    AZBLOB_ACCOUNT="insert-storage-account-name"
    AZBLOB_CONTAINER="insert-storage-account-container-name"

    # Login to azure
    az login

    # Set your default subscription 
    az acount set -s $SUBSCRIPTIONID

    # Create resource group
    az group create --name $RESOURCEGROUP --location $LOCATION

    # Create storage account
    az storage account create --name $AZBLOB_ACCOUNT --resource-group $RESOURCEGROUP --location $LOCATION --sku Standard_RAGRS --kind StorageV2

    # Create storage account container
    az storage container create --name $AZBLOB_CONTAINER --account-name $AZBLOB_ACCOUNT --auth-mode login
    ```

5. Create your .env file by copying .sample-env

    ```bash
    cp .sample-env .env
    ```

    and set up the following variables:

    - AZBLOB_ACCOUNT: The Azure Storage Account name
    - AZBLOB_CONTAINER: The Azure Storage Account container
    - AZBLOB_KEY: [OPTIONAL] The Azure Storage Account key. Leave this unset to authenticate with
      Microsoft Entra ID via `azidentity.DefaultAzureCredential` (e.g. `az login`, a Managed Identity, or a
      service principal via environment variables) instead, which is the recommended approach. Run
      `az storage blob generate-sas`-style role assignment (`Storage Blob Data Contributor`) on the account
      for whichever identity you use if you go this route.

6. Open your Terminal to run the sample

    ```bash
    go run .
    ```

7. In another terminal run the following curl command to enqueue Operations to the Batcher.

    ```bash
    curl http://localhost:8080/ingest
    ```

    You can also increase SharedCapacity by 1,000 by running the following curl command.

    ```bash
    curl http://localhost:8080/inc
    ```

    Likewise, you can decrease the SharedCapacity by 1,000 by running the following curl command.

    ```bash
    curl http://localhost:8080/dec
    ```
