FROM golang:1.9-alpine as go

RUN apk add --update git
RUN go get gitlab.com/cgps/bsonkit

COPY *.go /tmp/clustering/
RUN cd /tmp/clustering \
  && go build -o /cgps/clustering \
  && chmod +x /cgps/clustering \
  && rm -rf /tmp/clustering

WORKDIR /cgps

CMD ["/cgps/clustering"]
