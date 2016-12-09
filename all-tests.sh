docker run --entrypoint=/home/deploy/event-data-common/docker/all-tests.sh -p 9990:9990 -v `pwd`:/home/deploy/event-data-common -a stdout -it crossref/event-data-common-mock
