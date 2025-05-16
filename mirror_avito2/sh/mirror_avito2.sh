#!/usr/bin/env bash

dir=$(dirname "$0")
cd "$dir"

if [[ ! -e mirror_avito2 ]]; then
	echo "ERR: mirror_avito2 not found in folder '$dir'" >&2
	exit 1
fi

function killGeckodriver {
    is_exists_geckodriver=$(pgrep geckodriver)
    if [ $is_exists_geckodriver ]; then
        echo "-------------------------------"
        echo "kill geckodriver"
        kill -9 $is_exists_geckodriver
        killall -s 9 firefox
        rm -rf /tmp/rust_mozprofile*        
        sleep 15
    fi
}

# TODO: перезапуск гекодрайвера нужен только для команд "scan", для "send" гекодрайвер вообще не нужен 

killGeckodriver

echo "start geckodriver in background"
geckodriver &
sleep 15

echo ./mirror_avito2 "$@"
./mirror_avito2 "$@"

killGeckodriver

