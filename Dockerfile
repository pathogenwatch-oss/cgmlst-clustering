FROM golang:1.22.1-alpine

RUN apk add --no-cache --update gcc g++

COPY bin/go-get.sh /tmp/

COPY go.mod /tmp/clustering/

COPY go.sum /tmp/clustering/

COPY *.go /tmp/clustering/

ENV CGO_ENABLED=1

RUN cd /tmp/clustering \
  && go build -o /cgps/clustering \
  && chmod +x /cgps/clustering \
  && rm -rf /tmp/clustering

WORKDIR /cgps

CMD ["/cgps/clustering"]
