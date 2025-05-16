
### О программе
Сканер Авито 


### Установить geckodriver

```	
sudo apt update && sudo apt install -y firefox firefox-geckodriver
```

### Развернуть БД
```
/rust/mirror_avito2/sql/mirror_avito.sql
```

### Запустить сканирование 
Пример для участков Красноярска
```
./mirror_avito2.sh scan-lists -f krasnoyarsk-land-sale
```

