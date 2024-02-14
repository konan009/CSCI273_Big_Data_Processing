CREATE TABLE IF NOT EXISTS store.userpk (
  id UUID PRIMARY KEY,
  name TEXT,
  email set<TEXT>,
  mobile VARCHAR
);

INSERT INTO store.userpk (id,name, email, mobile) VALUES (uuid(),'test',{'one@test.com','two@test.com'},'123456789');

SELECT * FROM store.userpk;