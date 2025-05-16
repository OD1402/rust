#!/usr/bin/env bash
dir="$(dirname "$0")"
cd "$dir"

USER_AGENT="Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

declare -A vladis=( 
	# циан формат
	# ["cian"]="https://is.vladis.ru/advert-feed/d2233e14-5767-4cab-8703-70bebcdb31f8"
	# авито формат
	# ["avito"]="https://is.vladis.ru/advert-feed/810b7e1d-26fc-411e-a550-51626f953121"
	# яндекс формат (меньше объявлений)
	# ["domclick"]="https://is.vladis.ru/advert-feed/de4d3f44-4bbd-427d-97e8-69dd36685620"
	# яндекс формат (столько же объявлений, как в site)
	# ["zipal"]="https://prostor.vladis.ru/export_other_6vjio0bb5n"
	# яндекс формат (столько же объявлений, как в zipal)
	["site"]="https://prostor.vladis.ru/export_size_gjgxba9akp"
)

declare -A mlscenter=( 
	# циан формат
	# ["cian"]="http://mlscenter.ru/xml/converter/feeds/70b0695b02d8c13921c59c63da3c92ed.xml" 
	# авито формат
	# ["avito"]="http://mlscenter.ru/xml/converter/feeds/1f0b64c2fa9f75426f751d2b46c2c19c.xml"
	# авито формат  ???
	# ["yandex"]="http://mlscenter.ru/xml/converter/feeds/1f0b64c2fa9f75426f751d2b46c2c19c.xml"
	# яндекс формат
	["domclick"]="http://mlscenter.ru/xml/converter/feeds/ba2767c36448beb13d9e3f7f74f180fa.xml"
)

declare -A ogrk24=( 
	# яндекс формат
	["yandex"]="https://data.ogrk24.ru/xml/tt6yl3m-rfaa.xml"
	# циан формат
	# ["cian"]="https://data.ogrk24.ru/xml/free_flats_sell.xml"
)

declare -A etagi=( 
	# авито формат
	# ["avito"]="https://ecosystem.etagi.com/media/feeds/e9f469f3-94f2-4de4-8992-ee2d5cdba5c0_0.xml"
	# яндекс формат
	["yandex"]="https://ecosystem.etagi.com/media/feeds/c70f8e97-1484-4f36-b84c-0db7fc33b54c_0.xml"
	# авито формат
	# ["domclick"]="https://ecosystem.etagi.com/media/feeds/7a970035-4476-44a2-9817-4b83132e8c11_0.xml"
	# циан формат
	# ["1.cian"]="https://ecosystem.etagi.com/media/feeds/2f82ff60-4d20-4dc0-bebd-0353245e05f7_0.xml"
	# циан формат
	# ["2.cian"]="https://ecosystem.etagi.com/media/feeds/7a82363d-b123-4d9e-b7d5-5757242f6c2e_0.xml"
)

sources=(
	vladis
	mlscenter
	ogrk24
	etagi
)
set -e
for source in ${sources[@]}; do
    target_dir="raw/$source/$(date +%F)"
	declare -n src=$source
	for key in ${!src[@]}; do
		file_name="$key.xml"
		url="${src[$key]}"
		mkdir -p "$target_dir"
        echo "WILL fetch $url"
		curl "$url" -H "User-Agent: $USER_AGENT" -o "$target_dir/$file_name"
		gzip "$target_dir/$file_name"
        echo "DID fetch $url"
	done
done
