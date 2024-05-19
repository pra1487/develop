"""
SQL constraints:
================

Not Null
UNIQUE
Primarykey
Foreignkey
Check
Default

create table Emp(Eid int, Ename varchar(20), Gender char(1) Not Null, DOB Date)

create table Emp(Eid int, Ename varchar(20), Gender char(1) Not Null, Phno int UNIQUE);


create table department (Did int, Dname varchar (20), primary key(Did));

create table emp (eid int, ename varchar(50), Depin int, primary key(eid), foreign key(depid) references department(Did));

create table Emp(Eid int, Ename varchar(20), Age int check(age>25), Salary int check(Salary>50k),
primary key(Eid));

create table Emp(Eid int Not Null, Ename varchar(20), Age int Default 25, Salary int Default 50k,
projid int Default 1005S, primary key(Eid))

create table Emp(Eid int, Empname varchar(20), projid int, primary key(Eid),
foreign key(projid) references Projects(Pid) ON DELETE CASCADE);

create table Emp(Eid int, Empname varchar(20), projid int, primary key(Eid),
foreign key(projid) references Projects(Pid) ON DELETE SET NULL);

create table Emp(Eid int, Empname varchar(20), projid int DEFAULT 1001S, primary key(Eid),
foreign key(projid) references Projects(Pid) ON DELETE SET DEFAULT);

create table Emp(Eid int, Empname varchar(20), projid int, primary key(Eid),
foreign key(projid) references Projects(Pid) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT);

create table Emp(Eid int, Empname varchar(20), projid int, primary key(Eid),
foreign key(projid) references Projects(Pid) ON DELETE SET DEFAULT ON UPDATE SET NULL);

create table Emp(Eid int, Empname varchar(20), projid int, primary key(Eid),
foreign key(projid) references Projects(Pid) ON DELETE SET DEFAULT ON UPDATE CASCADE);


===================================================================


Concatenation:
---------------

Select Fname||Lname as Fullname from Emp;   # concatenate two columns

Select 'Fullname is'||Fname||Lname as Fullname from Emp;  # concatenate string and columns

Select Fname||' '||Lname as Fullname from Emp;

Select 'Age of '||Fname||' '||Lname||'is '||Age as EmployeeAge from Emp;



======================================================

Between:
--------

select * from Emp where age between age>=32 and age<=37;
or
Select * from Emp where age between 32 and 37;




"""