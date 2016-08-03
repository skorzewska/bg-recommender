#!/bin/bash

mkdir -p users

sed 's|^.*/collection/\(.*\)?rated=1$|\1|g' list >listlogins

logins=`cat listlogins`

try_again_msg='Please try again later for access.'

for login in ${logins}; do
    filename=users/${login}.xml
    echo ${filename}
    while [ ! -f ${filename} ] || grep -q "${try_again_msg}" ${filename}; do
        wget -O ${filename} "http://boardgamegeek.com/xmlapi/collection/${login}?rated=1"
        sleep $[ ( $RANDOM % 4 ) + 1 ]
    done
    sleep $[ ( $RANDOM % 4 ) + 1 ]
done
