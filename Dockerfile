##
## Copyright 2023 Ocean Protocol Foundation
## SPDX-License-Identifier: Apache-2.0
##
FROM python:3.8
LABEL maintainer="Ocean Protocol <devops@oceanprotocol.com>"
RUN apt-get update && apt-get install --no-install-recommends -y strace
COPY . /pdr-predictoor
WORKDIR /pdr-predictoor
RUN pip install -r requirements.txt
ENV RPC_URL="http://127.0.0.1:8545"
ENV SUBGRAPH_URL="http://172.15.0.15:8000/subgraphs/name/oceanprotocol/ocean-subgraph"
ENV PRIVATE_KEY="0xef4b441145c1d0f3b4bc6d61d29f5c6e502359481152f869247c7a4244d45209"
ENV BLOCKS_TILL_EPOCH_END=7
ENV WAIT_FOR_SUBGRAPH=false
ENV PYTHONUNBUFFERED=1
ENV DELAYED_STARTUP=1
ENTRYPOINT ["/pdr-predictoor/docker-entrypoint.sh"]
