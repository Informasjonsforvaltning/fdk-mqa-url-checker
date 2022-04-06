# fdk-mqa-url-checker

This service is part of the Metadata Quality Assessment stack. This service listens to dataset harvested events (Kafka) and 
checks every accessUrl and downloadUrl for all distributions in a dataset. Results are stored in a DQV metrics model which is
stored in the MQA event topic (Kafka).

## Install
Install CMake and CLang packages (Debian):

`sudo apt update og sudo apt install -y cmake clang`

Install Rust:

`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

## Code formatting

Format source code:

`rustfmt --edition 2021 src/*`

## Build
Build for development:

`cargo build --verbose`

Build release:

`cargo build --release`

## Run application
`./target/release/fdk-mqa-url-checker`

Show help:

`./target/release/fdk-mqa-url-checker --help`


## Kafka
Use this project to run your local Kafka cluster 

https://github.com/Informasjonsforvaltning/fdk-event-streaming-service
