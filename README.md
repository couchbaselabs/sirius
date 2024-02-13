# Readme

This Readme file provides instructions for building and running a Go application named **Sirius**. A distributed testing framework designed for Couchbase,
serving as a REST-based loading service for system, functional, performance and volume testing.

Doc loader different capabilities are described using  [**Rest Endpoints**](t-config.generated.md).

[Sirius APIs Demo](https://documenter.getpostman.com/view/25450208/2s93sdXWeB)
## Requirements

To build and run Sirius, you will need:

- Go
- Docker
- docker-compose

## Installation

Clone the repository:

```shell
git clone <repository_url>
```

Navigate to the cloned repository:

```shell
cd <repository_name>
```

Build Sirius:

```shell
make build
```

## Usage

### Running the Application Locally

To run Sirius locally, use the following command:

clean everything and run

```shell
make clean
```

Run

```shell
make run
```

### Deploying the Application with Docker

To deploy Sirius using Docker, use the following command:

```shell
make deploy
```

This command will stop any running Docker images, build and start Docker images for Sirius, and verify that the images
have been built and started.

To perform a fresh deployment, which will rebuild the Docker images, use the following command:

```shell
make fresh_deploy
```

### Stopping the Application

To stop Sirius, use the following command:

```shell
make down
```

## Cleaning

To clean up the t metadata and results directories, use the following command:

```shell
make clean
```

To clean up the directories and then run Sirius, use the following command:

```shell
make clean_run
```

To clean up the directories and then deploy Sirius, use the following command:

```shell
make clean_deploy
```

## Conclusion

Sirius is a simple but powerful Go application for loading Data into a server and storing t metadata and results. By
following the instructions in this Readme, you can quickly build and run the application locally, or deploy it to a
Docker environment for production use.

Internal Reference Only :-

[**Sirius**](https://docs.google.com/presentation/d/1B_de8lv1nKlaILmgGHwSUIVdRd4CYrAOa7rDV-d0QRA/edit#slide=id.g24f63dd352b_2_536)

[**Wiki**](https://couchbasecloud.atlassian.net/wiki/spaces/~6346ad4e62541f0d4c4f6785/pages/2000748801/Sirius)