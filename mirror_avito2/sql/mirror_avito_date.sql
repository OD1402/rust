-- old

-- CREATE OR REPLACE FUNCTION public.avito_date(s text)
--  RETURNS integer
--  LANGUAGE plpgsql
-- AS $function$
-- declare
-- 	_dt int;
-- 	_tm int;
-- 	_arr int[];
-- 	_tdt date;

-- 	_darr text[];
-- 	_year int;
-- 	_month int;
-- 	_day int;
-- begin
-- 	_dt = floor(extract(epoch from current_date::timestamp)) - 10800;
-- 	_tm = 0;
-- 	-- no use greedy!!! we need one row in answer!
-- 	-- _arr = regexp_matches(s, '(\d{2}):(\d{2})', 'g');
-- 	_arr = regexp_matches(s, '(\d{2}):(\d{2})');
-- 	if _arr[1] > 0 then
-- 		_tm = _arr[1]*3600 + _arr[2]*60;
-- 	end if;

-- 	if s LIKE '%Сегодня%' then
-- 		return _dt + _tm;
--   	elsif s LIKE '%Вчера%' then
-- 		return _dt + _tm - 24*3600;
-- 	else
-- 		_month = 1;
-- 		_day = 1;
-- 		_year = extract(year from current_timestamp);
-- 		-- no use greedy!!! we need one row in answer!
-- 		_darr = regexp_matches(s, '\"(\d+)\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s?(\d+)?');
	
-- 		if _darr[3] is not null then
-- 			_year = regexp_replace(_darr[3], '[^0-9]+', '', 'g')::int;
-- 			if _year < 100 then
-- 			_year = _year + 2000;
-- 			end if;
-- 		end if;
-- 		if _darr[2] = 'февраля' then
-- 			_month = 2;
-- 		elsif _darr[2] = 'марта' then
-- 			_month = 3;
-- 		elsif _darr[2] = 'марта' then
-- 			_month = 4;
-- 		elsif _darr[2] = 'мая' then
-- 			_month = 5;
-- 		elsif _darr[2] = 'июня' then
-- 			_month = 6;
-- 		elsif _darr[2] = 'июля' then
-- 			_month = 7;
-- 		elsif _darr[2] = 'августа' then
-- 			_month = 8;
-- 		elsif _darr[2] = 'сентября' then
-- 			_month = 9;
-- 		elsif _darr[2] = 'октября' then
-- 			_month = 10;
-- 		elsif _darr[2] = 'ноября' then
-- 			_month = 11;
-- 		elsif _darr[2] = 'декабря' then
-- 			_month = 12;
-- 		end if;
-- 		if _darr[1] is not null then
-- 			_day = regexp_replace(_darr[1], '[^0-9]+', '', 'g')::int;
-- 		end if;
	
-- 		if _month > extract(month from current_date::timestamp) then
-- 			_year = _year - 1;
-- 		end if;
	
-- 		if _darr[1] is not null then
-- 			_tdt = format('%s-%s-%s', _year, _month, _day)::date;
-- 			_dt = floor(extract(epoch from _tdt)) - 10800;
-- 		end if;
		 
-- 		return _dt + _tm;
-- 	end if;
-- 	return _dt + _tm;
-- end;
-- $function$
-- ;



-- DROP FUNCTION public.avito_date(text);

CREATE OR REPLACE FUNCTION public.avito_date(s text)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
	_dt int;
	_tm int;
	_arr int[];
	_tdt date;

	_darr text[];
	_year int;
	_month int;
	_day int;
	_days_ago int;

begin
	_dt = floor(extract(epoch from current_date::timestamp)) - 10800; 
	_tm = 0;
	-- no use greedy!!! we need one row in answer!
	-- _arr = regexp_matches(s, '(\d{2}):(\d{2})', 'g');
	_arr = regexp_matches(s, '(\d{2}):(\d{2})');
	if _arr[1] > 0 then
		_tm = _arr[1]*3600 + _arr[2]*60;
	end if;

	if s LIKE '%Сегодня%' then
		return _dt + _tm;
  	elsif s LIKE '%Вчера%' then
		return _dt + _tm - 24*3600;
	elsif s SIMILAR TO '%(день|дня|дней) назад%' then
		-- _days_ago = regexp_replace(s, '[^0-9]+', '', 'g')::int;
		_days_ago = substring(s from '"(\d)\s?(день|дня|дней) назад')::int;
		_dt = floor(extract(epoch from current_date::timestamp));
		
		if _days_ago > 0 then		
			return _dt - (_days_ago * 86400);
		end if;
	
	elsif s SIMILAR TO '%(недель|неделя|недели) назад%' then
		_days_ago = substring(s from '"(\d)\s?(недель|неделя|недели) назад')::int;
		_dt = floor(extract(epoch from current_date::timestamp));
		
		if _days_ago > 0 then		
			return _dt - (_days_ago * 86400 * 7);
		end if;
	
	elsif s SIMILAR TO '%(месяц|месяца|месяцев) назад%' then
		_days_ago = substring(s from '"(\d)\s?(месяц|месяца|месяцев) назад')::int;
		_dt = floor(extract(epoch from current_date::timestamp));
		
		if _days_ago > 0 then		
			return _dt - (_days_ago * 86400 * 7 * 4);
		end if;
			
	else
		_month = 1;
		_day = 1;
		_year = extract(year from current_timestamp);
		-- no use greedy!!! we need one row in answer!
		-- _darr = regexp_matches(s, '\"(\d+)\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s?(\d+)?');
		
		-- "title": "9 июня, 08:18"
		_darr = regexp_matches(s, '\"(\d+)\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря),?\s?(\d{2}):(\d{2})');

		if _darr[2] = 'февраля' then
			_month = 2;
		elsif _darr[2] = 'марта' then
			_month = 3;
		elsif _darr[2] = 'марта' then
			_month = 4;
		elsif _darr[2] = 'мая' then
			_month = 5;
		elsif _darr[2] = 'июня' then
			_month = 6;
		elsif _darr[2] = 'июля' then
			_month = 7;
		elsif _darr[2] = 'августа' then
			_month = 8;
		elsif _darr[2] = 'сентября' then
			_month = 9;
		elsif _darr[2] = 'октября' then
			_month = 10;
		elsif _darr[2] = 'ноября' then
			_month = 11;
		elsif _darr[2] = 'декабря' then
			_month = 12;
		end if;
		if _darr[1] is not null then
			_day = regexp_replace(_darr[1], '[^0-9]+', '', 'g')::int;
		end if;
	
		if _month > extract(month from current_date::timestamp) then
			_year = _year - 1;
		end if;
	
		if _darr[1] is not null then
			_tdt = format('%s-%s-%s', _year, _month, _day)::date;
			_dt = floor(extract(epoch from _tdt)) - 10800;
		end if;
		 
		return _dt + _tm;
	end if;

	return _dt + _tm;
end;
$function$
;

