# Event Data Common

<img src="doc/logo.png" align="right" style="float: right">

Common components for various services of Event Data. Tests run in Docker, but this is distributed as a Clojure library.

## To use

    [event-data-common "0.1.11"]

## Components

### Storage

A key-value storage protocol. Includes real storage interface for Redis and S3, and a test-only in-memory storage.

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

### Queue

Broadcast and recieve from ActiveMQ queues. Contains sender and listener with thread-local cached connection objects. The queue connection is configured upon construction, so this namespace does not directly use the configuration values.

## Testing

Unit tests:

  - `time docker-compose -f docker-compose-tests.yml run test lein test :unit`

Component tests:

  - `time docker-compose -f docker-compose-tests.yml run test lein test :component`

Integration tests require you to set environment variables `S3_KEY`, `S3_SECRET`, `S3_BUCKET_NAME`, `S3_REGION_NAME` in a `.env` file. NB this performs quite a lot of activity talking to AWS S3. An *empty* bucket should be provided.

 - `time docker-compose -f docker-compose-tests.yml run -w /usr/src/app test lein test :integration`

If the bucket is not empty, tests will still pass, but it may take a long time to clear the bucket. The AWS command-line tools provide a quick parallel way to empty a bucket:

 - `source .env && aws s3 rm --region $S3_REGION_NAME --recursive s3://$S3_BUCKET_NAME`

All tests:

These have the same requisites for integration tests.

  - `time docker-compose -f docker-compose-tests.yml run -w /usr/src/app test lein test :all`

## Configuration

The following configuration keys must be set in any code that uses these libraries:

 - GLOBAL_ARTIFACT_URL_BASE
 - GLOBAL_JWT_SECRETS
 - GLOBAL_KAFKA_BOOTSTRAP_SERVERS
 - GLOBAL_STATUS_TOPIC

## Distribution

To distribute:

 1. Tag release
 2. `lein deploy clojars`


## Development

To use use a local repository when developing new functionality against agents:

    lein clean && lein test && lein uberjar && rm -rf ~/.m2/repository/org.crossref & lein localrepo install target/uberjar/event-data-common-0.1.29-SNAPSHOT-standalone.jar event-data-common  "0.1.29-SNAPSHOT"
    
## License

Copyright © Crossref

Distributed under the The MIT License (MIT).
