#!/usr/bin/env bash
api_key_onlinesime="2198309e9c1a012b002bafc26a3075fb"
url="https://onlinesim.ru/api/proxy/getState.php?apikey=$api_key_onlinesime"
curl $url | jq . 

