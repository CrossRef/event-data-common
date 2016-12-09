# Event Data Common test image

FROM ubuntu
MAINTAINER Joe Wass jwass@crossref.org

RUN apt-get update
RUN apt-get -y install openjdk-8-jdk-headless
RUN apt-get -y install curl

RUN groupadd -r deploy && useradd -r -g deploy deploy

RUN curl https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein > /usr/bin/lein
RUN chmod a+x /usr/bin/lein

ADD . /code
RUN chown -R deploy /code
RUN mkdir  /home/deploy
RUN chown -R deploy /home/deploy


USER deploy

# Important to run as deploy, because every user gets a different lein installation.
RUN cd /code && lein compile