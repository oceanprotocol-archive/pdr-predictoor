# Setting up and Running PDR Predictoor on Sapphire test network

This guide explains the process of setting up and running the PDR Predictoor on the Sapphire test network and provides instructions on how to set it up locally, on Azure Container Instance and on Heroku for continuous execution.

## Initial Setup

### Getting Test Tokens

The Sapphire network is EVM (Ethereum Virtual Machine) compatible, you'll need to fund your EVM compatible wallet from the faucet in order to submit predictions to the smart contract.

- Go to [Oasis testnet faucet](https://faucet.testnet.oasis.dev/).
- Pick `Sapphire` from the dropdown.
- Fill in your address and request test tokens.

Nice! You got some test tokens. Next, get some testOCEAN tokens from the generous OCEAN faucet, these tokens will be used to stake:

- Go to the [testOCEAN faucet](https://faucet.sapphire.oceanprotocol.com/).
- Fill in your address and click `Get 1000 OCEAN`.

You're all set with a funded wallet!

### Repository and Environment Variables

#### Clone the repo

Clone the repo and move `.env.sample` to `.env`

```bash
git clone https://github.com/oceanprotocol/pdr-predictoor
mv .env.sample .env
```

#### Configure Environment variables

Edit the .env file and set the following environment variables:

- PRIVATE_KEY: Set it to the private key of the wallet you funded earlier.
- PAIR_FILTER: List of pairs to make predictions on (separated by comma), if empty the app will predict on all available pairs. For this example, set it to `"BTC/TUSD"`.
- TIMEFRAME_FILTER: Timeframes to predict on (separated by comma), if empty the app will predict on all available timeframes. For this example, set it to `5m`.
- SOURCE_FILTER: Price sources to predict on (separated by comma), if empty the app will predict on all available sources. For this example, set it to `BINANCE`
- STAKE_TOKEN: List of Token contract addresses to be used to stake, if empty the app will try to stake with any token. Set this to testOCEAN token address since that's the token you have in your wallet and what the prediction contract accepts: `"["0x973e69303259B0c2543a38665122b773D28405fB"]"`
- STAKE_AMOUNT: Determine the amount of tokens to stake if the confidence level is 100%. The final stake is calculated as (STAKE_AMOUNT * confidence / 100).
- RPC_URL: The RPC URL of the network, set this to Sapphire testnet Websocket RPC URL: `https://testnet.sapphire.oasis.dev`
- SUBGRAPH_URL: The Ocean subgraph url, set this to Sapphire testnet subgraph URL: `https://v4.subgraph.oasis-sapphire-testnet.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph/graphql`

### Configure the Prediction Model

The next step is to configure the prediction model. You can find a basic model within `pdr_predictoors/predictions/predict.py`.

To understand how it works, take a closer look at the following function:

```python
def predict_function(topic,contract_address, estimated_time):
```

This function is called each time a prediction is required with the following arguments:

- topic: The name of the pair to predict, e.g., BTC/TUSD-5m-Binance.
- estimated_time: The projected timestamp of the block being predicted. Please note that blockchain mining times may vary.

Function returns two variables:

- predicted_value: Boolean value indicating the predicted movement of the pair (up or down).
- predicted_confidence: An integer value between 1 and 100, representing the confidence level of the prediction. This value determines the stake (STAKE_AMOUNT * predicted_confidence/100) you are willing to put on your prediction.

If you do not wish to stake any tokens for the prediction, set the predicted_confidence to 0. If you want to ignore the prediction request, return None for both variables.

Feel free to link your existing model to this function or improve upon the provided example model.

## Running on any machine

Install the required packages and run the script.

```bash
# install virtualenv if it's not installed
pip install virtualenv

# activate the virtual environment

# on Unix
source venv/bin/activate
# on Windows
venv\Scripts\activate

# install the required packages
pip install -r requirements

# run the script
python main.py
```

You can set it up as a systemd service, use `pm2` or other solutions to continously keep `main.py` running. Here's how you can do it with `pm2`:

Firstly you need to install `pm2` globally, you can do it via npm.

```bash
npm install pm2 -g
```

Then, you can use `pm2 start` to run the script.

```bash
pm2 start main.py --name "pdr-predictoor"
```

Some other useful commands:

- `pm2 ls` - lists running processes
- `pm2 logs` - display the logs of all the running processes

You can find more [on pm2's official website](https://pm2.keymetrics.io/docs/usage/quick-start/)

## Build a container image

Make sure you have either Docker or Podman installed in your system. Run one of the following commands from the root directory of the repo:

```bash
# if docker is installed
docker build -t pdr-predictoor:latest .
# if podman is installed
podman build -t pdr-predictoor:latest .
```

This command builds the container image using the Dockerfile in the current directory and tags it as `pdr-predictoor:latest`. You can use any tag you'd like.

There are many options available for deploying the container image. Some of them include:
- [Heroku - Container Registry & Runtime (Docker Deploys)](https://devcenter.heroku.com/articles/container-registry-and-runtime)
- [AWS -  Deploy Docker Containers on Amazon ECS](https://aws.amazon.com/getting-started/hands-on/deploy-docker-containers/)
- [Google Cloud - Deploying to Cloud Run](https://cloud.google.com/run/docs/deploying)
- [Azure - Azure Container Instances (ACI)](#running-on-azure-as-a-container)

## Running on Azure as a Container

> **Warning**
> The information provided in this documentation is up to date as of June 2023. If you encounter any issues or need the latest instructions, please refer to [Azure's official documentation](https://learn.microsoft.com/en-us/azure/app-service/tutorial-custom-container?tabs=azure-cli&pivots=container-linux) on running containers.

In order to run the PDR Predictoor on Azure, you will have to build a container, upload it to Azure Container Registry, and finally run it using Azure Container Instances.

### Install Azure CLI

Follow the instructions from [Microsoft's website](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli#install) to install. Then type `az login` in the terminal to log-in.

### Build a container image

Follow the instructions in [Building a container image](#build-a-container-image) section.

### Setting up Azure Container Registry (ACR)

Next, create a container registry on Azure where you'll upload your container image.

[Click here to view the documentation for `az acr create`](https://learn.microsoft.com/en-us/cli/azure/acr?view=azure-cli-latest#az-acr-create)

```bash
az acr create --name <ACR_NAME> --resource-group <resource group name> --sku <sku>
```

- --name: The name of the container registry. It should be specified in lower case. You will need it in the next step.
- --resource-group: The name of the resource group where you want to create the ACR. You can learn more about managing resource groups from [Azure's documentation](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/manage-resource-groups-portal).
- --sku: Pricing tier for ACR, accepted values are: `Basic`, `Standard`, `Premium`.

### Pushing the container image to ACR

You can now push your container image to ACR. Don't forget to replace `<acr_name>` with the name you gave to your ACR in the previous step.


Firstly, you'll need to provide docker or podman with access to your ACR.

[Click here to view the documentation for `az acr login`](https://learn.microsoft.com/en-us/cli/azure/acr?view=azure-cli-latest#az-acr-login)

```bash
az acr login --name <acr_name>
```

> **Note**
> If you need to get access token you can use `--expose-token` parameter and login using `podman/docker login` command.
Next step is to push the container image.

```bash
# docker
docker tag pdr-predictoor:latest <acr_name>.azurecr.io/pdr-predictoor:latest
docker push <acr_name>.azurecr.io/pdr-predictoor:latest
```

```bash
# podman
podman tag pdr-predictoor:latest <acr_name>.azurecr.io/pdr-predictoor:latest
podman push <acr_name>.azurecr.io/pdr-predictoor:latest
```

The tag command is used to assign a new tag to your image. This is necessary because ACR requires a specific naming convention. After tagging the image, the `push` command is used to upload the image to the registry.

### Running the image on Azure Container Instances (ACI)

Finally, you can run your image as a container on ACI. Make sure to replace `<acr_name>` with the actual name of your ACR.

Create a container instance with 1 core and 1Gb of memory by running the following command:

[Click here to view the documentation for `az container create`](https://learn.microsoft.com/en-us/cli/azure/container?view=azure-cli-latest#az-container-create)

```bash
az container create --resource-group <resource-group-name> --name <container-instance-name> --image <acr-name>.azurecr.io/pdr-predictoor:latest --cpu 1 --memory 1
```

- --name: The name of the container instance. You will need it in the next step.
- --resource-group: The name of the resource group where you want to create the ACR. You can learn more about managing resource groups from [Azure's documentation](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/manage-resource-groups-portal).
- --image: The tag of the container image you've pushed to the registry in the previous step.

> **Note**
> You can set the enviroment variables defined in [Configure Enviroment Variables](#configure-environment-variables) step by passing a `--environment-variables <env-variables>` parameter to the `az container create` command.
Please see the [documentation](https://learn.microsoft.com/en-us/cli/azure/container?view=azure-cli-latest#az-container-create) to learn about all available commands.

### Monitoring the logs

To monitor the logs of your container, you can use `az container logs` command:

[Click here to view the documentation for `az container logs`](https://learn.microsoft.com/en-us/cli/azure/container?view=azure-cli-latest#az-container-logs)

```bash
az container logs --resource-group <resource-group-name> --name <container-instance-name>
```

### More

To access the list of available commands and detailed documentation, you can visit the [Azure documentation page for `az container`](https://learn.microsoft.com/en-us/cli/azure/container?view=azure-cli-latest). Alternatively, you can use the Azure Portal, which provides a GUI for managing your container instances. Through the portal you can easily perform tasks such as creating new instances, starting and stopping containers, scaling them etc.