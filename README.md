# Druid to IoTDB Data Transfer Tool

A high-performance Java application designed to transfer time-series data from Druid to Apache IoTDB. The tool supports multi-threaded operations, configurable batch processing, and robust error handling.

## Features

- Multi-threaded data reading and writing
- Configurable batch sizes for optimal performance
- Automatic schema validation and creation
- Robust error handling and retry mechanism
- Comprehensive logging system
- Connection pooling for IoTDB
- CSV-based tag configuration

## Prerequisites

- Java 8 or higher
- Maven 3.x
- Access to Druid source system
- Apache IoTDB instance

## Configuration

The application is configured through a `config.json` file in the root directory. Here's the structure:

```json
{
    "source": {
        "druid": {
            "api_url": "http://your-druid-server/api/data/current.do",
            "user_key": "your_user_key"
        },
        "time_range": {
            "start": "2024-01-01T00:00:00", 
            "end": "2024-01-01T06:00:00"
        },
        "tags_file": "tagList.csv"
    },
    "destination": {
        "iotdb": {
            "host": "localhost",
            "port": 6667,
            "username": "root",
            "password": "root",
            "session_pool_size": 10
        }
    },
    "processing": {
        "threads": {
            "reader_pool_size": 10,
            "writer_pool_size": 5
        },
        "batch": {
            "read_size": 100,
            "write_size": 500
        },
        "queue_size": 10000
    },
    "retry": {
        "initial_delay_ms": 1000,
        "max_delay_ms": 60000,
        "max_attempts": 5,
        "backoff_multiplier": 2.0
    }
}


### Configuration Parameters

#### Source Configuration
- `druid.api_url`: Druid API endpoint URL
- `druid.user_key`: Authentication key for Druid
- `time_range`: Data extraction time range
- `tags_file`: Path to CSV file containing tag definitions

#### Destination Configuration
- `iotdb`: IoTDB connection settings
- `session_pool_size`: Number of IoTDB sessions to maintain in the pool

#### Processing Configuration
- `reader_pool_size`: Number of concurrent reader threads
- `writer_pool_size`: Number of concurrent writer threads
- `read_size`: Batch size for reading from Druid
- `write_size`: Batch size for writing to IoTDB
- `queue_size`: Size of the internal data queue

#### Retry Configuration
- `initial_delay_ms`: Initial retry delay
- `max_delay_ms`: Maximum retry delay
- `max_attempts`: Maximum number of retry attempts
- `backoff_multiplier`: Exponential backoff multiplier

## Building the Project
```bash
mvn clean package
```

## Running the Application
```bash
java -jar target/iotdb-druid-connector-1.0-SNAPSHOT.jar
```


## Logging

The application uses Logback for logging with the following configuration:

- Console logging for all levels
- File-based logging with hourly rotation
- Separate error log file
- Logs are stored in the `logs/` directory

## Error Handling

- Failed writes are logged to a separate file for tracking
- Automatic retry mechanism with exponential backoff
- Comprehensive error logging and reporting

## Performance Tuning

Adjust the following parameters in `config.json` for optimal performance:

1. Increase `reader_pool_size` and `writer_pool_size` for more parallelism
2. Adjust `read_size` and `write_size` based on your data characteristics
3. Modify `queue_size` based on memory availability
4. Configure `session_pool_size` based on IoTDB server capacity