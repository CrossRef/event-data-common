# Event Data Common

Common components for various services of Event Data. Tests run in Docker, but this is distributed as a Clojure library.

## To use

    [event-data-common "0.1.8"]

## Components

### Storage

A key-value storage protocol.

### Redis storage

Implementation of storage for Redis plus other features like pub-sub for lightweight notifications.

### S3 storage

Implementation of storage for AWS S3.

### JWT verification

Verify that a JWT has been passed into a header. More than one secret can be supplied in configuration. Generate secrets.

### Status reporting

Send updates to the Status Service.

### Date and time

Various date and time functions, mostly connected to archiving.

### Backoff

Try and re-try functions in a threadpool. For robust connection to external systems.

## Testing

Unit tests:

  - `time docker-compose run -w /code test lein test :unit`

Component tests:

  - `time docker-compose run -w /code test lein test :component`

Integration tests require you to set environment variables `S3_KEY`, `S3_SECRET`, `S3_BUCKET_NAME`, `S3_REGION_NAME` in a `.env` file. NB this performs quite a lot of activity talking to AWS S3. An *empty* bucket should be provided.

 - `time docker-compose -f docker-compose-integration-tests.yml run -w /code test lein test :integration`

If the bucket is not empty, tests will still pass, but it may take a long time to clear the bucket. The AWS command-line tools provide a quick parallel way to empty a bucket:

 - `source .env && aws s3 rm --region $S3_REGION_NAME --recursive s3://$S3_BUCKET_NAME`

All tests:

These have the same requisites for integration tests.

- `time docker-compose run -f docker-compose-integration-tests.yml -w /code test lein test :all`

## Configuration

The following configuration keys must be set in any code that uses these libraries:

| Environment variable | Description                         | Default | Required for     |
|----------------------|-------------------------------------|---------|------------------|
| `REDIS_HOST`         | Redis host                          |         | Redis storage    |
| `REDIS_PORT`         | Redis port                          |         | Redis storage    |
| `REDIS_DB`           | Redis DB number                     | 0       | Redis storage    |
| `S3_KEY`             | AWS Key Id                          |         | S3 storage       | 
| `S3_SECRET`          | AWS Secret Key                      |         | S3 storage       |
| `S3_BUCKET_NAME`     | AWS S3 bucket name                  |         | S3 storage       |
| `S3_REGION_NAME`     | AWS S3 bucket region name           |         | S3 storage       |
| `STATUS_SERVICE`     | Public URL of the Status service    |         | Status Reporting |
| `JWT_SECRETS`        | Comma-separated list of JTW Secrets |         | JWT Verification |
| `ARTIFACT_BASE`      | URL base of Artifact Repository     |         | Artifact         |

## Distribution

To distribute:

 1. Tag release
 2. `lein deploy clojars`

## License

Copyright © Crossref

Distributed under the The MIT License (MIT).
