#!/bin/bash

TOKEN="QzPT@9izIa4rDMMHhyuBI9TI4"

curl -X PUT http://192.168.31.221:2668/locks/newlock -v \
  -H "Auth-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"ttl": 30 }'


echo ""
echo ""

sleep 10

echo ""
echo ""

curl -X DELETE http://192.168.31.221:2668/locks/newlock -v \
  -H "Auth-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"token": "01K8NZVNPM06VHTWZT3G5RNKZR"}'

sleep 5

echo ""
echo ""

curl -X DELETE http://192.168.31.221:2668/locks/newlock -v \
  -H "Auth-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"token": "01K8NZVNPM06VHTWZT3G5RNKZR"}'  