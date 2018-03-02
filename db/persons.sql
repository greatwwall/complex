create table persons
(
id int unsigned not null primary key auto_increment,
firstname varchar(30),
lastname varchar(30),
address varchar(200),
city varchar(30)
);
insert into persons values(1,'Gates', 'Bill', 'Ave 45th', 'NewYork');
insert into persons values(2,'Carter', 'Thomas', 'Changan Street', 'Mahathon');
insert into persons values(3,'Adams', 'John', 'Oxford Street', 'London');
insert into persons values(4,'Bush', 'George', 'Fifth Avenue', 'New York');
insert into persons values(5,'Obama', 'Barack', 'Pennsylvania Avenue', 'Washington');

insert into persons values(1,'Gates', 'Bill', 'Ave 45th', 'NewYork');
insert into persons values(2,'Carter', 'Thomas', 'Changan Street', 'Mahathon');
insert into persons values(3,'Adams', 'John', 'Oxford Street', 'London');
insert into persons values(4,'Bush', 'George', 'Fifth Avenue', 'New York');
insert into persons values(5,'Obama', 'Barack', 'Pennsylvania Avenue', 'Washington');

create table orders
(
ido int unsigned not null primary key auto_increment,
orderno number(5),
idp int unsigned
);

alter table orders add foreign key (idp) references persons(id);

insert into orders values(1, 77895, 3);
insert into orders values(2, 44678, 3);
insert into orders values(3, 22456, 1);
insert into orders values(4, 24562, 1);
insert into orders values(5, 34764, 5);

select * from persons inner join orders on persons.id = orders.idp where orders.idp=3;
select * from persons left join orders on persons.id = orders.idp;
select * from persons right join orders on persons.id = orders.idp;

select firstname, lastname into persons_backup from persons;


select idp,sum(orderno) from orders group by idp;
select idp, sum(orderno) from orders group by idp having sum(orderno) <100000;

SQL约束
NOT NULL，UNIQUE，PRIMARY KEY，FOREIGN KEY，CHECK，DEFAULT

ALTER TABLE Persons ADD UNIQUE (Id_P);
ALTER TABLE Persons ADD CONSTRAINT uc_PersonID UNIQUE (Id_P,LastName);

