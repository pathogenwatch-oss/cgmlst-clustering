#!/bin/bash
set -eu

CREDENTIALS=$1

if [[ ! -z $CREDENTIALS ]] ; then
  echo Setting git credentials.
  git config --global url.https://$CREDENTIALS@gitlab.com/.insteadOf https://gitlab.com/
else
  echo No git credentials provided.
fi

go get gitlab.com/cgps/bsonkit