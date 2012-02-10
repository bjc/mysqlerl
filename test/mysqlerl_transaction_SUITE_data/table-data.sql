DROP TABLE IF EXISTS user;

CREATE TABLE user (
       username VARCHAR(20) PRIMARY KEY NOT NULL,
       password VARCHAR(64)
);

INSERT INTO user (username, password)
       VALUES ('bjc', MD5('test'));
INSERT INTO user (username, password)
       VALUES ('siobain', MD5('test2'));
