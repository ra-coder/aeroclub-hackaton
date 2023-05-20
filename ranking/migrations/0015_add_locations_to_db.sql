create table cities
(
    id int primary key,
    CountryCode varchar(2),
    NameRu text,
    NameEn text,
    IataCode varchar(3),
    TimeZone decimal(6, 2)
);
create index on cities (id);
create index on cities (IataCode) where IataCode notnull;

select distinct TimeZone from cities;


create table airports (
    id int primary key,
    CityID int references cities,
    NameRu text,
    NameEn text,
    IATACode varchar(3)
);

create index on airports (id);
create index on airports (IataCode) where IataCode notnull;

create table iata_codes (
    code varchar(3) primary key,
    airport_id int references airports,
    city_id int references cities
);

create index on iata_codes (code);
create index on iata_codes (airport_id);
create index on iata_codes (city_id);


insert into iata_codes (code, airport_id, city_id)
select
    iatacode,
    null,
    id as city_id
from cities where IataCode notnull on conflict do nothing ;


insert into iata_codes (code, airport_id, city_id)
select
    iatacode,
    id,
    CityID
from airports where IataCode notnull on conflict do nothing;


insert into iata_codes(code, airport_id, city_id)
    values
       ('ЧАР', 263882, 60779),
       ('КЯС',37730,11265),
       ('СЕН',263770,306175),
       ('БОЧ',263867,15161),
       ('НЖГ', 62187,60698)
;
-- ЭК0226 ЧАРKJA 2022.09.17
-- ЭК0226 ЧАРKJA 2022.09.17
-- ЭК0162 СЕНKJA 2022.10.26
-- ЭК0362 СЕНКЯС 2022.07.01
-- ЭК0462 СЕНКЯС 2022.07.01
-- ЭК0188 БОЧКЯС 2022.09.30
