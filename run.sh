#!/bin/bash

# Configuration
COMPOSE_FILE="docker-compose.yaml"
ENV_FILE=".env"

# Display script usage instructions
show_help() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  etl    : Run ETL pipeline (Postgres & Airflow)"
    echo "  stream : Build and run streaming data generator (Kafka, Processor, Generator)"
    echo "  down   : Stop and remove all containers"
    echo ""
    echo "Examples:"
    echo "  $0 etl"
    echo "  $0 stream"
    exit 1
}

# Check if Docker Compose CLI is installed
if ! command -v docker compose &> /dev/null; then
    echo "Error: 'docker compose' is not installed or not in PATH."
    exit 1
fi

COMMAND=$1

# Display help menu if no command is provided
if [ -z "$COMMAND" ]; then
    show_help
fi

# Execute explicit workflow blocks
case "$COMMAND" in
    etl)
        echo "--> Build and run ETL..."
        if [ ! -f "$ENV_FILE" ]; then
            echo "Warning: $ENV_FILE file missing. Execution might fail."
        fi
        docker compose --env-file $ENV_FILE up -d postgres airflow-webserver airflow-scheduler airflow-init
        ;;
    stream)
        echo "--> Building images and run stream data generator..."
        docker compose up -d --build kafka kafka-init processor generator
        ;;
    down)
        echo "--> Shutdown all services..."
        docker compose down
        ;;
    *)
        echo "Error: Command '$COMMAND' is not recognized."
        show_help
        ;;
esac
