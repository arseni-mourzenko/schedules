drop table if exists users;
drop table if exists events;
create table users (id serial primary key, slots bytea);
create table events (id serial primary key, slots bytea);
create index idx_slots on users using gin (slots);