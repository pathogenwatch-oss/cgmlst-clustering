FROM golang:1.20.2-alpine as go

ARG git_credentials

COPY bin/go-get.sh /tmp/

COPY *.mod /tmp/clustering/

RUN cd /tmp/clustering \
  && apk add --update -t git-deps git bash \
  && /tmp/go-get.sh $git_credentials \
  && apk del --purge git-deps

COPY *.go /tmp/clustering/

RUN cd /tmp/clustering \
  && go build -o /cgps/clustering \
  && chmod +x /cgps/clustering \
  && rm -rf /tmp/clustering

WORKDIR /cgps

CMD ["/cgps/clustering"]
