# Readme

This Readme file provides instructions for building and running a Go application named Sirius. The Sirius 
application is designed to load data into a Couchbase server and store task metadata and results in directories.

Doc loader different capabilities are  described using  [**REST ENDPOINT**](task-config.generated.md).

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
make build_sirius
```

## Usage

### Running the Application Locally

To run Sirius locally, use the following command:

clean everything and  run
```shell
make clean_run
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

This command will stop any running Docker images, build and start Docker images for Sirius, and verify that the images have been built and started.

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

To clean up the task metadata and results directories, use the following command:

```shell
make clean_dir
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

Sirius is a simple but powerful Go application for loading data into a server and storing task metadata and results. By following the instructions in this Readme, you can quickly build and run the application locally, or deploy it to a Docker environment for production use.
