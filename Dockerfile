FROM golang:1.20.2-alpine as go

ARG git_credentials

COPY bin/go-get.sh /tmp/

COPY go.mod /tmp/clustering/

COPY go.sum /tmp/clustering/

COPY *.go /tmp/clustering/

RUN cd /tmp/clustering \
  && go build -o /cgps/clustering \
  && chmod +x /cgps/clustering \
  && rm -rf /tmp/clustering

WORKDIR /cgps

CMD ["/cgps/clustering"]
