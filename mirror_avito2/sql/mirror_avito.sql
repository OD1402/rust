--Установите сертификат:
--mkdir -p ~/.postgresql && \
--wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O ~/.postgresql/root.crt && \
--chmod 0600 ~/.postgresql/root.crt

--Пример строки подключения:
--psql "host=rc1b-9bf69xl8t4oczfwf.mdb.yandexcloud.net \
--      port=6432 \
--      sslmode=verify-full \
--      dbname=mirror_avito \
--      user=mirror_avito \
--      target_session_attrs=read-write"
--pass: 1014103mirror_avito

--psql 'postgres://admin:nug4Laih@46.138.253.194:54495/postgres?sslmode=disable'
--create database mirror_avito; -- https://www.postgresql.org/docs/12/sql-createdatabase.html 
--drop user mirror_avito;
--create user mirror_avito with password 'NuT7KQrq'; -- https://www.postgresql.org/docs/8.0/sql-createuser.html
--psql 'postgres://mirror_avito:NuT7KQrq@46.138.253.194:54495/mirror_avito?sslmode=disable'

--grant connect on database mirror_avito to mirror_avito; -- https://tableplus.com/blog/2018/04/postgresql-how-to-grant-access-to-users.html
--grant usage on schema public to mirror_avito; 
--GRANT ALL PRIVILEGES ON DATABASE mirror_avito TO mirror_avito;

drop table if exists facets cascade;
create table facets(
	id smallserial primary key, -- https://habr.com/ru/company/tensor/blog/507688/
	value text not null unique
);
drop table if exists scan_sessions cascade;
create table scan_sessions(
	id serial primary key,
	started_at timestamptz not null,
	finished_at timestamptz,
	facet smallint not null references facets(id) on delete cascade
);

drop function if exists new_scan_session(text, timestamptz);
create function new_scan_session(_facet text, _started_at timestamptz) 
returns int as $$
declare 
	_id int;
	_ret int;
begin
	select id into _id from facets where value = _facet;
	if _id is null then
		insert into facets (value) values (_facet)
		returning id into _id;
	end if;
	insert into scan_sessions (started_at, facet) 
	values (_started_at, _id)
	returning id into _ret;
	return _ret;
end;
$$ language plpgsql;

drop function if exists new_scan_session_desktop(text, timestamptz);
create function new_scan_session_desktop(_facet text, _started_at timestamptz) 
returns int as $$
declare 
	_id int;
	_ret int;
begin
	select id into _id from facets where value = _facet;
	if _id is null then
		insert into facets (value) values (_facet)
		returning id into _id;
	end if;

	select id into _ret from scan_sessions where 
		facet = _id AND 
		finished_at IS NULL AND
		DATE(started_at) > (NOW() - interval '2 day')
		ORDER BY started_at desc limit 1;

	if _ret is null then
		insert into scan_sessions (started_at, facet) 
		values (_started_at, _id)
		returning id into _ret;
	end if;

	return _ret;
end;
$$ language plpgsql;

drop function if exists start_scan_session(text);
create function start_scan_session(_facet text) 
returns table (
	scan_session_id int,
	started_at timestamptz,
	min_price bigint,
	page_num smallint,
	already_scanned_count int
) as $$
declare 
--	_interval interval = interval '30 minutes';
	_interval interval = interval '3 hours';
	_facet_id int;
	_scan_session_id int;
	_ret int;
	_min_price bigint;
	_scan_diap_id int;
	_page_num smallint;
	_started_at timestamptz; 
	_finished_at timestamptz;
	_already_scanned_count int = 0;
begin
	select into _facet_id id  
	from facets where value = _facet;
	if _facet_id is null then
		insert into facets (value) values (_facet)
		returning id into _facet_id;
	end if;
	select into _scan_session_id, _started_at, _finished_at
		ss.id, ss.started_at, ss.finished_at  
	from scan_sessions ss 
	where ss.facet = _facet_id 
	order by ss.id desc;
	if _facet = 'Краснодарский край::Квартира::Купить' then
		_interval = interval '3 days';
	end if;
	if _finished_at is null and (
		select max(sp.created_at)
		from scan_diaps sd
		left join scan_pages sp on sp.scan_diap = sd.id
		where sd.scan_session = _scan_session_id 
	) > now() - _interval then
		select into _scan_diap_id, _min_price
			distinct on(sd.min_price)
			id, sd.min_price
		from scan_diaps sd
		where sd.scan_session = _scan_session_id 
		order by sd.min_price desc;

		select into _page_num max(sp.page_num)
		from scan_pages sp
		where sp.scan_diap = _scan_diap_id;

		select into _already_scanned_count count(*) from (
			select unnest(sp.list_shots)
			from scan_diaps sd
			left join scan_pages sp on sp.scan_diap = sd.id 
			where sd.scan_session = _scan_session_id and not (
				sp.scan_diap = _scan_diap_id and sp.page_num = _page_num 
			)
		) t;

	else
		_started_at = now();
		insert into scan_sessions (started_at, facet) 
		values (_started_at, _facet_id)
		returning id into _scan_session_id;
	end if;
	return query select 
		_scan_session_id as scan_session_id, 
		_started_at as started_at,
		_min_price as min_price, 
		_page_num as page_num,
		_already_scanned_count;
end;
$$ language plpgsql;
select * from start_scan_session('Краснодарский край::Квартира::Купить');

drop table if exists scan_diaps cascade;
create table scan_diaps (
	id serial primary key,
	scan_session int not null references scan_sessions(id) on delete cascade,
	min_price bigint not null,
	unique (scan_session, min_price)
);
drop function if exists new_scan_diap(int, bigint);
create function new_scan_diap(_scan_session int, _min_price bigint) returns int as $$
declare
	_id int;
begin
	select id into _id 
		from scan_diaps  
		where scan_session = _scan_session and min_price = _min_price;
	if _id is null then
		insert into scan_diaps  
				   ( scan_session,  min_price) 
			values (_scan_session, _min_price)
			on conflict (scan_session, min_price) 
			do update
			set min_price = excluded.min_price
			returning id into _id;
	end if;
	return _id;
end;
$$ language plpgsql;

drop table if exists scan_pages cascade;
create table scan_pages(
	id serial primary key,
	created_at timestamptz not null,
	scan_diap int not null references scan_diaps(id) on delete cascade,
	page_num smallint not null,
	list_shots int[],
	avito_count int not null,
	unique (scan_diap, page_num)
);
alter table scan_pages add column updated_at timestamptz;
drop table if exists list_shots cascade;
create table list_shots (
	id serial primary key,
	created_at timestamptz not null,
	updated_at timestamptz,
	avito_id bigint not null,
	avito_time int not null,
	value jsonb not null,
	unique(avito_id, avito_time)
);
alter table list_shots add column avito_price bigint;
update list_shots 
set avito_price = case when '' = regexp_replace(coalesce(value#>'{value,price,current}',value#>'{value,price}',value#>'{priceDetailed,fullString}')::text, '[^0-9]+', '', 'g') then 0
	when length(regexp_replace(coalesce(value#>'{value,price,current}',value#>'{value,price}',value#>'{priceDetailed,fullString}')::text, '[^0-9]+', '', 'g')) > 18 then -1
	else (regexp_replace(coalesce(value#>'{value,price,current}',value#>'{value,price}',value#>'{priceDetailed,fullString}')::text, '[^0-9]+', '', 'g'))::bigint
end
where avito_price is null;
alter table list_shots alter column avito_price set not null;
alter table list_shots drop constraint list_shots_avito_id_avito_time_key;
alter table list_shots add constraint list_shots_avito_id_avito_time_avito_price_key unique (avito_id, avito_time, avito_price);

drop function if exists save_scan_page(timestamptz, int, bigint, smallint, jsonb[], int);
create function save_scan_page(
	_created_at timestamptz,
	_scan_session int, 
	_min_price bigint,
	_page_num smallint,
	_list_shots jsonb[], 
	_avito_count int) returns int as $$
declare
	_ids int[] = '{}';
	_id int;
	_avito_id bigint;
	_avito_time int;
	_avito_price bigint;
	_value jsonb;
	_scan_diap int;
	_s text;	
begin
	select new_scan_diap(_scan_session, _min_price) into _scan_diap;
	foreach _value in array _list_shots loop
		_avito_id = _value#>'{value,id}';
		if _avito_id is null then
           _avito_id = _value#>'{id}';
        end if;
		_avito_time = _value#>'{value,time}';
		if _avito_time is null then
           _avito_time = _value#>'{time}';
        end if;
		if _avito_time is null then
         	_s = _value#>> '{value,freeForm,0,content,leftChildren,0,content,children}';
         	_avito_time = avito_date(_s);
        end if;        

		if _avito_time > 1000000000 then
			_avito_time = _avito_time / 60;
		end if;
		case when '' = regexp_replace(coalesce(_value#>'{value,price,current}',_value#>'{value,price}')::text, '[^0-9]+', '', 'g') then 
				_avito_price = 0;
			when length(regexp_replace(coalesce(_value#>'{value,price,current}',_value#>'{value,price}')::text, '[^0-9]+', '', 'g')) > 18 then 
				_avito_price = -1;
			else 
				_avito_price = (regexp_replace(coalesce(_value#>'{value,price,current}',_value#>'{value,price}')::text, '[^0-9]+', '', 'g'))::bigint;
		end case;
		select id into _id 
			from list_shots 
			where avito_id = _avito_id and avito_time = _avito_time and avito_price = _avito_price;
		if _id is not null then
			update list_shots 
				set value = _value, updated_at = _created_at 
				where avito_id = _avito_id and avito_time = _avito_time and avito_price = _avito_price;
		else
			insert into list_shots 
					   ( created_at,  avito_id,  avito_time,  avito_price,  value) 
				values (_created_at, _avito_id, _avito_time, _avito_price, _value)
				on conflict (avito_id, avito_time, avito_price) 
				do update
				set 
					value = _value, 
					updated_at = _created_at
				returning id into _id;
		end if;
		_ids = array_append(_ids, _id);
	end loop;

	select into _id id 
	from scan_pages 
	where scan_diap = _scan_diap and page_num = _page_num;
	if _id is not null  then
		update scan_pages
		set 
			updated_at = _created_at, 
			list_shots = _ids, 
			avito_count = _avito_count 
		where id = _id;
	else
		insert into scan_pages
			   ( created_at,  scan_diap,  page_num,  list_shots, avito_count)
		values (_created_at, _scan_diap, _page_num, _ids,       _avito_count)
		on conflict (scan_diap, page_num)
		do update
		set updated_at = _created_at, list_shots = _ids, avito_count = _avito_count  
		returning id into _id;
	end if;
	return _id;
end;
$$ language plpgsql;


drop function if exists save_scan_page_desktop(timestamptz, int, bigint, smallint, jsonb[], int);
create function save_scan_page_desktop(
	_created_at timestamptz,
	_scan_session int, 
	_min_price bigint,
	_page_num smallint,
	_list_shots jsonb[], 
	_avito_count int) returns int as $$
declare
	_ids int[] = '{}';
	_id int;
	_avito_id bigint;
	_avito_time bigint;
	_avito_price bigint;
	_value jsonb;
	_scan_diap int;	
begin
	select new_scan_diap(_scan_session, _min_price) into _scan_diap;
	foreach _value in array _list_shots loop
		_avito_id = _value#>'{id}';
		_avito_time = _value#>'{sortTimeStamp}';
		if _avito_time > 1000000000000 then
			_avito_time = _avito_time / 1000;
		end if;
		if _avito_time > 1000000000 then
			_avito_time = _avito_time / 60;
		end if;
		case when '' = regexp_replace(coalesce(_value#>'{priceDetailed,fullString}')::text, '[^0-9]+', '', 'g') then 
				_avito_price = 0;
			when length(regexp_replace(coalesce(_value#>'{priceDetailed,fullString}')::text, '[^0-9]+', '', 'g')) > 18 then 
				_avito_price = -1;
			else 
				_avito_price = (regexp_replace(coalesce(_value#>'{priceDetailed,fullString}')::text, '[^0-9]+', '', 'g'))::bigint;
		end case;
		select id into _id 
			from list_shots 
			where avito_id = _avito_id and avito_time = _avito_time and avito_price = _avito_price;
		if _id is not null then
			update list_shots 
				set value = _value, updated_at = _created_at 
				where avito_id = _avito_id and avito_time = _avito_time and avito_price = _avito_price;
		else
			insert into list_shots 
					   ( created_at,  avito_id,  avito_time,  avito_price,  value) 
				values (_created_at, _avito_id, _avito_time, _avito_price, _value)
				on conflict (avito_id, avito_time, avito_price) 
				do update
				set 
					value = _value, 
					updated_at = _created_at
				returning id into _id;
		end if;
		_ids = array_append(_ids, _id);
	end loop;

	select into _id id 
	from scan_pages 
	where scan_diap = _scan_diap and page_num = _page_num;
	if _id is not null  then
		update scan_pages
		set 
			updated_at = _created_at, 
			list_shots = _ids, 
			avito_count = _avito_count 
		where id = _id;
	else
		insert into scan_pages
			   ( created_at,  scan_diap,  page_num,  list_shots, avito_count)
		values (_created_at, _scan_diap, _page_num, _ids,       _avito_count)
		on conflict (scan_diap, page_num)
		do update
		set updated_at = _created_at, list_shots = _ids, avito_count = _avito_count  
		returning id into _id;
	end if;
	return _id;
end;
$$ language plpgsql;

drop table if exists card_shots cascade;
create table card_shots (
	id serial primary key,
	created_at timestamptz not null,
	updated_at timestamptz,
	avito_id bigint not null,
	avito_time int not null,
	value jsonb,
	unique(avito_id, avito_time)
);
alter table card_shots add column avito_price bigint;
update card_shots 
set avito_price = case when '' = regexp_replace((value#>'{price,value}')::text, '[^0-9]+', '', 'g') then 0
	when length(regexp_replace((value#>'{price,value}')::text, '[^0-9]+', '', 'g')) > 18 then -1
	else (regexp_replace((value#>'{price,value}')::text, '[^0-9]+', '', 'g'))::bigint
end 
where avito_price is null;
alter table card_shots alter column avito_price set not null;
alter table card_shots drop constraint card_shots_avito_id_avito_time_key;
alter table card_shots add constraint card_shots_avito_id_avito_time_avito_price_key unique (avito_id, avito_time, avito_price);

drop procedure if exists save_card_shot(timestamptz, jsonb);
create procedure save_card_shot(
	_created_at timestamptz,
	_value jsonb) as $$
declare
	_id int;
	_avito_id bigint;
	_avito_time int;
	_avito_price bigint;
begin
	_avito_id = (_value->>'id')::bigint;
	_avito_time = (_value->>'time')::int / 60;
	case when '' = regexp_replace((_value#>'{price,value}')::text, '[^0-9]+', '', 'g') then 
			_avito_price = 0;
		when length(regexp_replace((_value#>'{price,value}')::text, '[^0-9]+', '', 'g')) > 18 then 
			_avito_price = -1;
		else 
			_avito_price = (regexp_replace((_value#>'{price,value}')::text, '[^0-9]+', '', 'g'))::bigint;
	end case;
	select id into _id 
		from card_shots 
		where avito_id = _avito_id and avito_time = _avito_time and avito_price = _avito_price;
	if _id is not null then
		update card_shots 
			set value = _value, updated_at = _created_at 
			where avito_id = _avito_id and avito_time = _avito_time and avito_price = _avito_price;
	else
		insert into card_shots 
			   ( created_at,  avito_id,  avito_time,  avito_price,  value) 
		values (_created_at, _avito_id, _avito_time, _avito_price, _value)
		on conflict (avito_id, avito_time, avito_price) 
		do update
		set 
			value = _value, 
			updated_at = _created_at
		returning id into _id;
	end if;
end;
$$ language plpgsql;
--call save_card_shot(
--	now(), 
--	'{"id":1,"time":360}'::jsonb);
--select * from card_shots cs where avito_id = 1;
--delete from card_shots where avito_time < 1000;
--select min(avito_time), max(avito_time) from card_shots cs;
--update card_shots set avito_time = avito_time / 60 where avito_time > 1000000000;

drop procedure if exists save_cards(timestamptz, jsonb[]);
create procedure save_cards(_created_at timestamptz, _cards jsonb[])
as $$
declare
	_value jsonb;
	_ok jsonb;
	_err jsonb;
	_avito_id bigint;
	_status smallint;
begin
	foreach _value in array _cards loop
		_ok = _value->'ok';
		if _ok is not null then
			perform save_card_shot(_created_at, _ok);
		else 
			_err = _value->'err';
			if _err is null then
				raise exception 'mirror_avito.save_cards: nor "ok", neither "err" found at _value = %', _value;
			else 
				_avito_id = (_err->>'id')::bigint;
				_status = (_err->>'status')::smallint;
				perform save_card_shot_failed(_created_at, _avito_id, _status);
			end if;
		end if;
	end loop;
end;
$$ language plpgsql;
--call save_cards(
--	now(), 
--	array[ 
--		'{"id":1,"time":999}'::jsonb, 
--		'{"id":2,"status":404}'::jsonb 
--	]
--);
--call save_cards(
--	now(), 
--	array[ 
--		'{"ok": {"id":1,"time":999}}'::jsonb, 
--		'{"err": {"id":2,"status":404}}'::jsonb 
--	]
--);
--select * from card_shots cs where avito_id = 1;
--select * from card_shots_failed csf where avito_id = 2;

drop table if exists card_shots_failed cascade;
create table card_shots_failed (
	id serial primary key,
	created_at timestamptz not null,
	updated_at timestamptz,
	avito_id bigint not null,
	status smallint not null,
	unique(avito_id)
);
drop procedure if exists save_card_shot_failed(timestamptz, bigint, smallint);
create procedure save_card_shot_failed(
	_created_at timestamptz,
	_avito_id bigint, 
	_status smallint) as $$
declare
	_id int;
begin
	select id into _id 
		from card_shots_failed
		where avito_id = _avito_id;
	if _id is not null then
		update card_shots_failed 
			set status = _status, updated_at = _created_at 
			where avito_id = _avito_id;
	else
		insert into card_shots_failed 
			   ( created_at,  avito_id,  status) 
		values (_created_at, _avito_id, _status)
		on conflict (avito_id) 
		do update
		set 
			status = _status, 
			updated_at = _created_at
		returning id into _id;
	end if;
end;
$$ language plpgsql;
--call save_card_shot_failed(
--	now(), 
--	37::bigint,
--	404::smallint);
--call save_card_shot_failed(
--	now(), 
--	13::bigint,
--	500::smallint);

drop function if exists len_of_last_scan_session(text, bool);
create function len_of_last_scan_session(_facet text, _finished bool) returns int 
as $$ 
	select count(*) 
	from (
		select unnest(sp.list_shots)
		from scan_sessions ss 
		left join scan_diaps sd on scan_session = ss.id
		left join scan_pages sp on scan_diap = sd.id
		where ss.id = (
			select id from scan_sessions_of_facet(_facet, _finished) limit 1
		)
	) as ids
$$ language sql;

drop function if exists ulen_of_last_scan_session(text, bool);
create function ulen_of_last_scan_session(_facet text, _finished bool) returns int 
as $$ 
	select count(distinct avito_id) 
	from (
		select unnest(sp.list_shots) as id
		from scan_sessions ss 
		left join scan_diaps sd on scan_session = ss.id
		left join scan_pages sp on scan_diap = sd.id
		where ss.id = (
			select id from scan_sessions_of_facet(_facet, _finished) limit 1
		)
	) as ids
	left join list_shots ls on ls.id = ids.id
$$ language sql;

drop function if exists avito_count_of_last_scan_session(text, bool);
create function avito_count_of_last_scan_session(_facet text, _finished bool) 
returns int 
as $$ 
	select distinct on(sd.id, sp.id) sp.avito_count 
	from scan_sessions ss 
	left join scan_diaps sd on scan_session = ss.id
	left join scan_pages sp on scan_diap = sd.id
	where ss.id = (
		select id from scan_sessions_of_facet(_facet, _finished) limit 1
	)
	order by sd.id asc, sp.id desc
$$ language sql;

drop function if exists describe_last_scan_session(text, bool, int, text);
create function describe_last_scan_session(
	_facet text, 
	_finished bool,
	_scan_session_relative_to int,
	_scan_deep_interval text
)
returns table (
	id int,
	started_at timestamptz,
	finished_at timestamptz,
	avito_count int,
	len int,
	ulen int,
	list_shots_to_send_all int,
	list_shots_to_send_relative int,
	list_shots_new int,
	list_shots_changed int,
	cards_to_scan int,
	card_shots_to_send int
) as $$
	select 
		t.id, t.started_at, t.finished_at,
		avito_count_of_last_scan_session(_facet, _finished) as avito_count,
		len_of_last_scan_session(_facet, _finished) as len,
		ulen_of_last_scan_session(_facet, _finished) as ulen,
		(select count(*) from list_shots_to_send_all(_facet, _finished)) as list_shots_to_send_all,
		(select count(*) from list_shots_to_send_relative(_facet, _scan_session_relative_to)) as list_shots_to_send_relative,
		(
			select count(*) 
			from list_shots_relative(_facet, _scan_session_relative_to)
			where id_prev is null
		) as list_shots_new,
		(
			select count(*) 
			from list_shots_relative(_facet, _scan_session_relative_to)
			where 
				id_prev is not null and (
					avito_time_prev < avito_time_last or 
					avito_price_prev != avito_price_last
				)
		) as list_shots_changed,
		(select count(*) from cards_to_scan(_facet, _finished)) as cards_to_scan,
		(select count(*) from card_shots_to_send(_facet, _finished, _scan_deep_interval)) as card_shots_to_send
	from (select * from scan_sessions_of_facet(_facet, _finished) limit 1) t 
$$ language sql;
select * from describe_last_scan_session('Московская область::Квартира::Купить', true, null, null);
select * from describe_last_scan_session('Московская область::Квартира::Купить', true, null, '24 hours');
select * from describe_last_scan_session('Москва::Комната::Купить', true, null);
select * from describe_last_scan_session('Москва::Комната::Снять', true, null);
select * from describe_last_scan_session('Москва::Коммерческая недвижимость::Купить', true, null);
select * from scan_sessions_of_facet('Московская область::Квартира::Купить', true); --where finished_at is null;

--select 
--		avito_id,
--		avito_price_list,
--		avito_price_card,
--		coalesce(updated_at_list, created_at_list) scanned_at_list,
--		coalesce(updated_at_card, created_at_card) scanned_at_card,
--		value_list, 
--		value_card
--	from (
--		select distinct on(t.avito_id) 
--			t.avito_id,
--			cs.avito_time as avito_time_card, 
--			cs.avito_price as avito_price_card,
--			t.avito_time as avito_time_list, 
--			t.avito_price as avito_price_list
--			, t.created_at as created_at_list
--			, t.updated_at as updated_at_list
--			, cs.created_at as created_at_card
--			, cs.updated_at as updated_at_card
--			, t.value as value_list
--			, cs.value as value_card
--		from (
--			select distinct on(ls.avito_id) 
--				ls.avito_id, 
--				ls.avito_time, 
--				ls.avito_price, 
--				ls.created_at,
--				ls.updated_at,
--				ls.value
--			from (
--				select unnest(sp.list_shots) as id
--				from scan_sessions ss 
--				left join scan_diaps sd on scan_session = ss.id
--				left join scan_pages sp on scan_diap = sd.id
--				where ss.id = (
--					select id from scan_sessions_of_facet('Москва::Коммерческая недвижимость::Купить', true) limit 1
--				)
--			) as ids 
--			left join list_shots ls on ls.id = ids.id 
--			order by ls.avito_id, ls.avito_time desc, ls.created_at desc
--		) t
--		left join card_shots cs on cs.avito_id = t.avito_id
--		left join card_shots_failed csf on csf.avito_id = t.avito_id
--		where 
--			csf.created_at is null or 
--			coalesce(csf.updated_at, csf.created_at) < coalesce(t.updated_at, t.created_at)
--		order by t.avito_id, cs.avito_time desc, coalesce(cs.updated_at, cs.created_at) desc
--	) tt
--	where 
--		(avito_time_card is null or avito_time_card < avito_time_list) or 
--		(avito_price_card is null or avito_price_card != avito_price_list);
	

drop function if exists session_diff;
create function session_diff(
	_scan_session_left int, 
	_scan_session_right int) returns 
table (
	avito_id bigint
)
as $$
	select ta.avito_id 
	from (
		select distinct avito_id 
		from (
			select unnest(sp.list_shots) as id
			from scan_sessions ss 
			left join scan_diaps sd on scan_session = ss.id
			left join scan_pages sp on scan_diap = sd.id
			where ss.id = _scan_session_left
		) as ids
		left join list_shots ls on ls.id = ids.id
	) ta
	left join (
		select distinct avito_id 
		from (
			select unnest(sp.list_shots) as id
			from scan_sessions ss 
			left join scan_diaps sd on scan_session = ss.id
			left join scan_pages sp on scan_diap = sd.id
			where ss.id = _scan_session_right
		) as ids
		left join list_shots ls on ls.id = ids.id
	) tb on tb.avito_id = ta.avito_id 
	where tb.avito_id is null
$$ language sql;
--select count(*) from session_diff(1, 15);
--select count(*) from session_diff(15, 1);

drop function if exists cards_to_scan(text, bool);
create or replace function cards_to_scan(
	_facet text, 
	_finished bool
) returns table (
	avito_id bigint
)
as $$ 
	select avito_id
	from (
		select distinct on(t.avito_id) 
			t.avito_id,
			cs.avito_time as avito_time_card, 
			cs.avito_price as avito_price_card,
			coalesce(cs.updated_at, cs.created_at) scanned_at_card,
			t.avito_time as avito_time_list, 
			t.avito_price as avito_price_list,
			t.scanned_at as scanned_at_list
		from (
			select distinct on(ls.avito_id) 
				ls.avito_id, 
				ls.avito_time, 
				ls.avito_price, 
				coalesce(ls.updated_at, ls.created_at) scanned_at
			from list_shots_to_send_all(_facet, _finished) ids
			left join list_shots ls on ls.id = ids.id
			order by ls.avito_id, ls.avito_time desc, ls.created_at desc
		) t
		left join card_shots cs on cs.avito_id = t.avito_id
		left join card_shots_failed csf on csf.avito_id = t.avito_id
		where 
			csf.created_at is null 
			or coalesce(csf.updated_at, csf.created_at) < t.scanned_at and csf.status != 404
		order by t.avito_id, cs.avito_time desc, coalesce(cs.updated_at, cs.created_at) desc
	) tt
	where 
		(avito_time_card is null or avito_time_card < avito_time_list) 
		or 
		(avito_price_card is null or avito_price_card != avito_price_list and scanned_at_card < scanned_at_list)
	order by avito_id desc
--	select avito_id
--	from (
--		select distinct on(t.avito_id) 
--			t.avito_id,
--			cs.avito_time as avito_time_card, 
--			cs.avito_price as avito_price_card,
--			t.avito_time as avito_time_list, 
--			t.avito_price as avito_price_list,
--			t.created_at as created_at_list
--		from (
--			select distinct on(ls.avito_id) 
--				ls.avito_id, 
--				ls.avito_time, 
--				ls.avito_price, 
--				ls.created_at,
--				ls.updated_at
--			from list_shots_to_send_all(_facet, _finished) 
--			left join list_shots ls on ls.id = ids.id 
--			order by ls.avito_id, ls.avito_time desc, ls.created_at desc
--		) t
--		left join card_shots cs on cs.avito_id = t.avito_id
--		left join card_shots_failed csf on csf.avito_id = t.avito_id
--		where 
--			csf.created_at is null or 
--			coalesce(csf.updated_at, csf.created_at) <= coalesce(t.updated_at, t.created_at)
--		order by t.avito_id, cs.avito_time desc, coalesce(cs.updated_at, cs.created_at) desc
--	) tt
--	where 
--		(avito_time_card is null or avito_time_card < avito_time_list) or 
--		(avito_price_card is null or avito_price_card != avito_price_list)
$$ language sql;
select count(*) from cards_to_scan('Московская область::Квартира::Купить'::text, true);
select count(*) from cards_to_scan('Москва::Квартира::Купить'::text, true);

--select count(*) from (
--	select avito_id
--	from (
--		select distinct on(t.avito_id) 
--			t.avito_id,
--			cs.avito_time as avito_time_card, 
--			cs.avito_price as avito_price_card,
--			coalesce(cs.updated_at, cs.created_at) scanned_at_card,
--			t.avito_time as avito_time_list, 
--			t.avito_price as avito_price_list,
--			t.scanned_at as scanned_at_list
--		from (
--			select distinct on(ls.avito_id) 
--				ls.avito_id, 
--				ls.avito_time, 
--				ls.avito_price, 
--				coalesce(ls.updated_at, ls.created_at) scanned_at
--			from list_shots_to_send_all('Московская область::Квартира::Купить'::text, true) ids
--			left join list_shots ls on ls.id = ids.id
--			order by ls.avito_id, ls.avito_time desc, ls.created_at desc
--		) t
--		left join card_shots cs on cs.avito_id = t.avito_id
--		left join card_shots_failed csf on csf.avito_id = t.avito_id
--		where 
--			csf.created_at is null 
--			or coalesce(csf.updated_at, csf.created_at) < t.scanned_at
--		order by t.avito_id, cs.avito_time desc, coalesce(cs.updated_at, cs.created_at) desc
--	) tt
--	where 
--		(avito_time_card is null or avito_time_card < avito_time_list) 
--		or 
--		(avito_price_card is null or avito_price_card != avito_price_list and scanned_at_card < scanned_at_list)
--) t3;
--select count(*) from list_shots_to_send_all('Московская область::Квартира::Купить'::text, true);

-- занимательный пример:
	select 
		avito_id,
		avito_price_list,
		avito_price_card,
		coalesce(updated_at_list, created_at_list) scanned_at_list,
		coalesce(updated_at_card, created_at_card) scanned_at_card,
		value_list, 
		value_card
	from (
		select distinct on(t.avito_id) 
			t.avito_id,
			cs.avito_time as avito_time_card, 
			cs.avito_price as avito_price_card,
			t.avito_time as avito_time_list, 
			t.avito_price as avito_price_list
			, t.created_at as created_at_list
			, t.updated_at as updated_at_list
			, cs.created_at as created_at_card
			, cs.updated_at as updated_at_card
			, t.value as value_list
			, cs.value as value_card
		from (
			select distinct on(ls.avito_id) 
				ls.avito_id, 
				ls.avito_time, 
				ls.avito_price, 
				ls.created_at,
				ls.updated_at,
				ls.value
			from (
				select unnest(sp.list_shots) as id
				from scan_sessions ss 
				left join scan_diaps sd on scan_session = ss.id
				left join scan_pages sp on scan_diap = sd.id
				where ss.id = (
					select id from scan_sessions_of_facet('Москва::Комната::Снять', true) limit 1
				)
			) as ids 
			left join list_shots ls on ls.id = ids.id 
			order by ls.avito_id, ls.avito_time desc, ls.created_at desc
		) t
		left join card_shots cs on cs.avito_id = t.avito_id
		left join card_shots_failed csf on csf.avito_id = t.avito_id
		where 
			csf.created_at is null or 
			coalesce(csf.updated_at, csf.created_at) < coalesce(t.updated_at, t.created_at)
		order by t.avito_id, cs.avito_time desc, coalesce(cs.updated_at, cs.created_at) desc
	) tt
	where 
		(avito_time_card is null or avito_time_card < avito_time_list) or 
		(avito_price_card is null or avito_price_card != avito_price_list);


select count(*)
from cards_to_scan('Москва::Квартира::Купить'::text, true) cts
left join card_shots cs on cs.avito_id = cts.avito_id
where cs.avito_id  is null;

select *
from cards_to_scan('Москва::Коммерческая недвижимость::Купить'::text, true) cts
join card_shots_failed csf on csf.avito_id = cts.avito_id;


drop function if exists list_shots_to_send_all(text, bool);
create function list_shots_to_send_all(
	_facet text, 
	_finished bool
) returns table (
	id int
)
as $$
	select distinct id from (
		select unnest(sp.list_shots) as id
		from scan_sessions ss 
		left join scan_diaps sd on scan_session = ss.id
		left join scan_pages sp on scan_diap = sd.id
		where ss.id = (
			select id from scan_sessions_of_facet(_facet, _finished) limit 1
		)
	) t
$$ language sql;
select count(*) from list_shots_to_send_all('Москва::Квартира::Купить', true);

drop function if exists list_shots_relative(text, int);
create function list_shots_relative(
	_facet text, 
	_scan_session_relative_to int
) returns table (
	avito_id bigint,
	id_last int,
	id_prev int,
	avito_time_last int,
	avito_time_prev int,
	avito_price_last bigint,
	avito_price_prev bigint
)
as $$
	select 
		tt.avito_id,
		tt.id id_last,
		t3.id id_prev,
		tt.avito_time avito_time_last,
		t3.avito_time avito_time_prev,
		tt.avito_price avito_price_last,
		t3.avito_price avito_price_prev
	from (
		select distinct on (ls.avito_id) ls.*
		from (
			select unnest(sp.list_shots) as id
			from scan_sessions ss 
			left join scan_diaps sd on scan_session = ss.id
			left join scan_pages sp on scan_diap = sd.id
			where ss.id = (
				select id from scan_sessions_of_facet(_facet, true) limit 1
			)
		) t
		join list_shots ls on ls.id = t.id
		order by ls.avito_id, ls.avito_time desc, coalesce(ls.updated_at,ls.created_at) desc
	) tt
	left join (
		select distinct on (ls.avito_id) ls.*
		from (
			select unnest(sp.list_shots) as id
			from scan_sessions ss 
			left join scan_diaps sd on scan_session = ss.id
			left join scan_pages sp on scan_diap = sd.id
			where ss.id = coalesce(
				_scan_session_relative_to, 
				( select id from scan_sessions_of_facet(_facet, true) offset 1 limit 1)
			)
		) t
		join list_shots ls on ls.id = t.id
		order by ls.avito_id, ls.avito_time desc, coalesce(ls.updated_at,ls.created_at) desc
	) t3 on t3.avito_id = tt.avito_id
$$ language sql;

drop function if exists list_shots_to_send_relative(text, int);
create function list_shots_to_send_relative(
	_facet text, 
	_scan_session_relative_to int
) returns table (
	id int
)
as $$
	select id_last 
	from list_shots_relative(_facet, _scan_session_relative_to)
	where 
		id_prev is null or 
		avito_time_prev < avito_time_last or 
		avito_price_prev != avito_price_last
$$ language sql;
select count(*) from list_shots_to_send_relative('Москва::Квартира::Купить', null);
select count(*) from list_shots_to_send_relative('Москва::Квартира::Купить', 488);
select count(*) from list_shots_to_send_all('Москва::Квартира::Купить', true);

drop function if exists scan_sessions_of_facet(text, bool);
create function scan_sessions_of_facet(_facet text, _finished bool) 
returns table (
	id int,
	started_at timestamptz,
	finished_at timestamptz
) as $$
	select ss.id, ss.started_at, ss.finished_at  
	from scan_sessions ss 
	join facets f on f.id = ss.facet 
	where f.value = _facet and (not _finished or finished_at is not null)
	order by ss.id desc
$$ language sql;
select * from scan_sessions_of_facet('Москва::Дом, дача, коттедж::Купить', false); 

select count(*) from list_shots_to_send_all('Москва::Квартира::Купить', true);

select count(*) from card_shots_to_send('Москва::Квартира::Купить', true, '90 day');

select count(*) from card_shots_to_send('Москва::Квартира::Купить', true, '2 day');

select count(*) from card_shots_to_send('Москва::Квартира::Купить', true, '4 day');

drop function if exists card_shots_to_send(text, bool, text);
create function card_shots_to_send(
	_facet text, 
	_finished bool,
	_scan_deep_interval text
) returns table (
	id int
)
as $$
	select id_cs
	from (
		select distinct on(cs.avito_id) 
			cs.id id_cs, 
			cs.avito_time avito_time_cs, 
			coalesce(cs.updated_at, cs.created_at) scanned_at_cs,
			ls.id id_ls,
			coalesce(ls.updated_at, ls.created_at) scanned_at_ls,
			ls.avito_time avito_time_ls
		from list_shots_to_send_all(_facet, _finished) t
		join list_shots ls on ls.id = t.id
		join card_shots cs on ls.avito_id = cs.avito_id 
		order by cs.avito_id, cs.avito_time desc, cs.id desc
	) t
	where 
		scanned_at_cs >= coalesce(now() - _scan_deep_interval::interval, scanned_at_ls) and 
		avito_time_cs >= avito_time_ls 
$$ language sql;
--select count(*) from card_shots_to_send('Московская область::Квартира::Купить', true, null);
--select count(*) from card_shots_to_send('Московская область::Квартира::Купить', true, '24 hours');

--select count(*) from (
--		select distinct on(cs.avito_id) 
--			cs.id id_cs, 
--			cs.avito_time avito_time_cs, 
--			cs.created_at created_at_cs, 
--			cs.updated_at updated_at_cs, 
--			ls.id id_ls,
--			ls.created_at created_at_ls,
--			ls.updated_at updated_at_ls,
--			ls.avito_time avito_time_ls
--		from list_shots_to_send_all('Московская область::Квартира::Купить', true) t
--		join list_shots ls on ls.id = t.id
--		join card_shots cs on ls.avito_id = cs.avito_id 
--		order by cs.avito_id, cs.id desc
--	) t;
--
--select avito_id from (
--	select distinct on(cs.avito_id) 
--		ls.avito_id,
--		cs.avito_time avito_time_cs,
--		ls.avito_time avito_time_ls,
--		coalesce(cs.updated_at, cs.created_at) scanned_at_cs,
--		coalesce(ls.updated_at, ls.created_at) scanned_at_ls
--	from (
--		select * 
--		from list_shots_relative('Московская область::Квартира::Купить', null)
--		where id_prev is null
--	) t 
--	join list_shots ls on ls.id = t.id_last
--	join card_shots cs on cs.avito_id = ls.avito_id 
--	order by cs.avito_id, cs.avito_time desc
--) t
--where avito_time_cs >= avito_time_ls 
--and scanned_at_cs < scanned_at_ls
--;
--
--		select * 
--		from list_shots_relative('Московская область::Квартира::Купить', null)
--		where id_prev is null and avito_id = 607895253;
--
--select * from list_shots ls where avito_id = 607895253;
--select * from card_shots ls where avito_id = 607895253;
----select * from scan_sessions ss join facets f on f.id = ss.facet where f.value = 'Московская область::Квартира::Купить' order by ;
--select * from scan_sessions_of_facet('Московская область::Квартира::Купить', false); -- 745, 719
--
--select distinct on (ls.avito_id) ls.*
--		from (
--			select unnest(sp.list_shots) as id
--			from scan_sessions ss 
--			left join scan_diaps sd on scan_session = ss.id
--			left join scan_pages sp on scan_diap = sd.id
--			where ss.id = 719
--		) t
--		join list_shots ls on ls.id = t.id
--		where ls.avito_id = 607895253
--		order by ls.avito_id, ls.avito_time desc, coalesce(ls.updated_at,ls.created_at) desc;
--
--select count(distinct avito_id) 
--	from list_shots_relative('Московская область::Квартира::Купить', null)
--where id_prev is null;

--	select 
--		tt.avito_id,
--		tt.id id_last,
--		t3.id id_prev,
--		tt.avito_time avito_time_last,
--		t3.avito_time avito_time_prev,
--		tt.avito_price avito_price_last,
--		t3.avito_price avito_price_prev
--	from (
--		select distinct on (ls.avito_id) ls.*
--		from (
--			select unnest(sp.list_shots) as id
--			from scan_sessions ss 
--			left join scan_diaps sd on scan_session = ss.id
--			left join scan_pages sp on scan_diap = sd.id
--			where ss.id = 745
--		) t
--		join list_shots ls on ls.id = t.id
--		order by ls.avito_id, ls.avito_time desc, coalesce(ls.updated_at,ls.created_at) desc
--	) tt
--	left join (
--		select distinct on (ls.avito_id) ls.*
--		from (
--			select unnest(sp.list_shots) as id
--			from scan_sessions ss 
--			left join scan_diaps sd on scan_session = ss.id
--			left join scan_pages sp on scan_diap = sd.id
--			where ss.id = coalesce(
--				_scan_session_relative_to, 
--				( select id from scan_sessions_of_facet(_facet, true) offset 2 limit 1)
--			)
--		) t
--		join list_shots ls on ls.id = t.id
--		order by ls.avito_id, ls.avito_time desc, coalesce(ls.updated_at,ls.created_at) desc
--	) t3 on t3.avito_id = tt.avito_id;
--
--select id from scan_sessions_of_facet('Московская область::Квартира::Купить', true) offset 1 limit 1;


select count(*) from (
	select id_cs
	from (
		select distinct on(cs.avito_id) 
			cs.id id_cs, 
			cs.avito_time avito_time_cs, 
			cs.created_at created_at_cs, 
			cs.updated_at updated_at_cs, 
			ls.id id_ls,
			ls.created_at created_at_ls,
			ls.updated_at updated_at_ls,
			ls.avito_time avito_time_ls
		from list_shots_to_send_all('Московская область::Квартира::Купить', true) t
		join list_shots ls on ls.id = t.id
		join card_shots cs on ls.avito_id = cs.avito_id 
		order by cs.avito_id, cs.id desc
	) t
	where 
		coalesce(updated_at_cs, created_at_cs) > coalesce(updated_at_ls, created_at_ls) and 
		avito_time_cs >= avito_time_ls 
) tt

drop function if exists list_shots_to_send(int[]);
create function list_shots_to_send(_ids int[]) 
returns table (
	value jsonb
)
as $$
	select value from list_shots where id = any(_ids);
$$ language sql;
--select count(*) from list_shots_to_send('{1,2,3}');
--select array_agg(value) as list_shots from list_shots_to_send('{1,2,3}');

drop function if exists card_shots_to_send(int[]);
create function card_shots_to_send(_ids int[]) 
returns table (
	value jsonb
)
as $$
	select value from card_shots where id = any(_ids);
$$ language sql;
--select count(*) from card_shots_to_send('{1,2,3}');
--select array_agg(value) as card_shots from card_shots_to_send('{1,2,3}');

drop function if exists deleted(bigint[]);
create function deleted(_avito_ids bigint[]) 
returns bigint[] 
as $$
	select array_agg(ids.avito_id) 
	from ( 
		select unnest(_avito_ids) as avito_id 
	) as ids
	left join (
		select ls.avito_id from (
			select unnest(sp.list_shots) as id
			from scan_sessions ss 
			left join scan_diaps sd on sd.scan_session = ss.id
			left join scan_pages sp on sp.scan_diap = sd.id
			where ss.id in (
				select distinct on(ss.facet) ss.id 
				from scan_sessions ss 
				where ss.finished_at is not null
				order by ss.facet, ss.id desc --https://stackoverflow.com/questions/13325583/postgresql-max-and-group-by
			)
		) as tt
		left join list_shots ls on ls.id = tt.id
	) as t on t.avito_id = ids.avito_id
	where t.avito_id is null;
$$ language sql;

drop function if exists deleted2(bigint[]);
create function deleted2(_avito_ids bigint[]) 
returns table (
	avito_id bigint
)
as $$
	select ids.avito_id 
	from ( 
		select unnest(_avito_ids) as avito_id 
	) as ids
	left join (
		select ls.avito_id from (
			select unnest(sp.list_shots) as id
			from scan_sessions ss 
			left join scan_diaps sd on sd.scan_session = ss.id
			left join scan_pages sp on sp.scan_diap = sd.id
			where ss.id in (
				select distinct on(ss.facet) ss.id 
				from scan_sessions ss 
				where ss.finished_at is not null
				order by ss.facet, ss.id desc --https://stackoverflow.com/questions/13325583/postgresql-max-and-group-by
			)
		) as tt
		left join list_shots ls on ls.id = tt.id
	) as t on t.avito_id = ids.avito_id
	where t.avito_id is null;
$$ language sql;

drop function if exists session_diff;
create function session_diff(
	_scan_session_left int, 
	_scan_session_right int) returns 
table (
	avito_id bigint
)
as $$
	select ta.avito_id 
	from (
		select distinct avito_id 
		from (
			select unnest(sp.list_shots) as id
			from scan_sessions ss 
			left join scan_diaps sd on scan_session = ss.id
			left join scan_pages sp on scan_diap = sd.id
			where ss.id = _scan_session_left
		) as ids
		left join list_shots ls on ls.id = ids.id
	) ta
	left join (
		select distinct avito_id 
		from (
			select unnest(sp.list_shots) as id
			from scan_sessions ss 
			left join scan_diaps sd on scan_session = ss.id
			left join scan_pages sp on scan_diap = sd.id
			where ss.id = _scan_session_right
		) as ids
		left join list_shots ls on ls.id = ids.id
	) tb on tb.avito_id = ta.avito_id 
	where tb.avito_id is null
$$ language sql;

drop function if exists which_facet(bigint);
create function which_facet(_avito_id bigint) returns 
	table (
		id smallint,
		value text
	) 
as $$
	select f.id, f.value
	from list_shots ls
	left join scan_pages sp on ls.id = any(sp.list_shots)
	left join scan_diaps sd on sd.id = sp.scan_diap 
	left join scan_sessions ss on ss.id = sd.scan_session 
	left join facets f on f.id = facet
	where ls.avito_id = _avito_id
	group by f.id, f.value
$$ language sql;

drop function if exists which_session(bigint);
create function which_session(_avito_id bigint) returns 
	table (
		id smallint,
		started_at timestamptz,
		finished_at timestamptz 
	) 
as $$
	select ss.id, ss.started_at, ss.finished_at 
	from list_shots ls
	left join scan_pages sp on ls.id = any(sp.list_shots)
	left join scan_diaps sd on sd.id = sp.scan_diap 
	left join scan_sessions ss on ss.id = sd.scan_session 
	where ls.avito_id = _avito_id
	order by ss.started_at desc
$$ language sql;

