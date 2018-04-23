FROM golang:1.9-alpine as go

ARG git_credentials
COPY bin/git-config.sh .
RUN apk add --update -t git-deps git bash \
  && ./git-config.sh $git_credentials \
  && go get gitlab.com/cgps/bsonkit \
  && apk del --purge git-deps

COPY *.go /tmp/clustering/
RUN cd /tmp/clustering \
  && go build -o /cgps/clustering \
  && chmod +x /cgps/clustering \
  && rm -rf /tmp/clustering

WORKDIR /cgps

CMD ["/cgps/clustering"]
