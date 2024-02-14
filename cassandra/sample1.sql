CREATE KEYSPACE IF NOT EXISTS store
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE IF NOT EXISTS store.users (
  id INT PRIMARY KEY,
  name TEXT,
  email set<TEXT>,
  mobile VARCHAR
);

BEGIN BATCH
  INSERT INTO store.users (id,name, email, mobile) VALUES (0,'Brian Baker',{'brianbaker@email.com'},'(800) 555‑0175');
  INSERT INTO store.users (id,name, email, mobile) VALUES (1,'Mona Lott',{'monalott@email.com'},'(415) 555‑0132');
  INSERT INTO store.users (id,name, email, mobile) VALUES (2,'Ivana Tinkle',{'ivanatinkle@email.com'},'(63) 961-194-0123');
  INSERT INTO store.users (id,name, email, mobile) VALUES (3,'Anita Bath',{'anitabath@email.com'},'(5) 153‑0132');
  INSERT INTO store.users (id,name, email, mobile) VALUES (4,'Eileen Dover',{'eileendover@email.com'},'(415) 153‑0132');
APPLY BATCH;

INSERT INTO store.users (id,name, email, mobile) VALUES (5,'test',{'one@test.com','two@test.com'},'123456789');

SELECT * FROM store.users;
