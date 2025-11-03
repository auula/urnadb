#!/bin/bash

curl -X PUT http://192.168.31.221:2668/variants/num-01 \
  -H "Auth-Token: FWxQak2rdxWnw45AlGn7R955t" \
  -H "Content-Type: application/json" \
  -d '{"variant": 0 }'


wrk -t10 -c100 -d10s -s post.lua http://192.168.31.221:2668/variants/num-01


curl -X GET http://192.168.31.221:2668/variants/num-01 \
  -H "Auth-Token: FWxQak2rdxWnw45AlGn7R955t"