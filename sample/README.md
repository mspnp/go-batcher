# Usage - Code sample

This code sample demonstrates the usage of the `go-batcher`.

## Prerequisites

- [Go v1.15+](https://golang.org/)
- [Visual Studio Code](https://code.visualstudio.com/)
- [Azure Subscription](https://azure.microsoft.com/en-us/free/)

## Guide

1. Clone this repo [https://github.com/plasne/go-batcher](https://github.com/plasne/go-batcher)
2. Open the `/sample` directory in Visual Studio Code
3. Update `sample/go.mod` with your go version
4. Update the `replace` command in `sample/go.mod` with your local path for go-batcher

    ```go
    replace github.com/plasne/go-batcher => <local-path-for-go-batcher>
    ```

5. Create an [Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-create?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&tabs=azure-cli) and a container. This will be the used for the AzureSharedResource rate limiter. Using [Azure CLI](https://docs.microsoft.com/en-gb/cli/azure/install-azure-cli) you can use the following commands:

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

6. Create your .env file by copying .sample-env

    ```bash
    cp .sample-env .env
    ```

    and set up the following variables:

    - AZBLOB_ACCOUNT: The Azure Storage Account name
    - AZBLOB_CONTAINER: The Azure Storage Account container
    - AZBLOB_KEY: The Azure Storage Account key

7. Open your Terminal to run the sample

    ```bash
    go run .
    ```

8. In another terminal run the following curl command to enqueue Operations to the Batcher.

    ```bash
    curl http://localhost:8080/ingest
    ```
