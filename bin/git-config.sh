#!/bin/bash
set -eu

$CREDENTIALS=$1

if [ ! -z $CREDENTIALS ] ; then
  git config --global url.https://$CREDENTIALS@gitlab.com/.insteadOf git://gitlab.com/
fi
