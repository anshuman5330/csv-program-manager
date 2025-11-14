# csv-program-manager
A Python-based solution to process CSV files from a folder and stream their content to a Kafka topic, with robust error handling, logging, and Docker deployment support.


Features of this app:

Reads configuration details from config.ini

Fetches Kafka broker and topic from environment variables

Validates and processes only .csv files

Moves processed files to:
    archive/ (on success)
    error/ (on failure)

Creates missing folders automatically

Implements dual logging:

Console output

Rotating file logs (app.log)