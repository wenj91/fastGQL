## fastGQL

Realtime GraphQL on PostgreSQL / MySQL

### About

This project exposes PostgreSQL / MySQL through GraphQL API, with built-in authorization engine. Inspired by [Hasura](https://hasura.io/).

This is alpha-version, with some features still in development:

- [x] basic operations
  - [x] query
    - [x] query_by_pk
    - [x] query
    - [x] query_count
    - [x] query_aggregate
  - [x] mutate
    - [x] insert
    - [x] update
  - [x] delete
  - [ ] subscription
- [x] authorization engine configured with custom interceptor
- [x] user-defined filtering via GraphQL arguments
  - [x] where
  - [x] order by
  - [x] limit
  - [x] offset
  - [x] distinct on(PostgreSQL: distinct on, MySQL: distinct)
- [x] integration [graphql-spqr](https://github.com/leangen/graphql-spqr)

### Example

#### Full example

module: `fastgql-example`

#### Database

```mysql
CREATE TABLE addresses(
  id INT PRIMARY KEY,
  street VARCHAR(255)
);

CREATE TABLE customers(
  id INT PRIMARY KEY,
  name VARCHAR(255),
  address INT REFERENCES addresses(id)
);

INSERT INTO addresses VALUES (0, 'Danes Hill');
INSERT INTO addresses VALUES (1, 'Rangewood Road');
INSERT INTO customers VALUES (0, 'Stacey', 0);
INSERT INTO customers VALUES (1, 'John', 0);
INSERT INTO customers VALUES (2, 'Daniele', 1);

CREATE TABLE `test` ( 
  `id` INT AUTO_INCREMENT NOT NULL,
  `name` VARCHAR(50) NULL DEFAULT NULL ,
  `age` INT NULL DEFAULT NULL ,
  `create_time` TIMESTAMP NULL DEFAULT current_timestamp() ,
  `update_time` TIMESTAMP NULL DEFAULT NULL ,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`id`)
);

INSERT INTO `test` (`id`, `name`, `age`, `create_time`, `update_time`) VALUES (1, 'test', 111, '2021-07-24T01:09:42.000Z', NULL);
INSERT INTO `test` (`id`, `name`, `age`, `create_time`, `update_time`) VALUES (2, 'test2', 111, '2021-07-24T01:09:42.000Z', NULL);
INSERT INTO `test` (`id`, `name`, `age`, `create_time`, `update_time`) VALUES (3, 'test3', 111, '2021-07-24T01:09:42.000Z', NULL);


CREATE TABLE `t_user` ( 
  `id` INT AUTO_INCREMENT NOT NULL,
  `name` VARCHAR(50) NULL DEFAULT NULL ,
  `password` VARCHAR(100) NULL DEFAULT NULL ,
  `create_time` DATETIME NULL DEFAULT NULL ,
  `test_id` INT NULL DEFAULT NULL ,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`id`)
);

INSERT INTO `t_user` (`id`, `name`, `password`, `create_time`, `test_id`) VALUES (1, 'user', 'password', NULL, 2);
INSERT INTO `t_user` (`id`, `name`, `password`, `create_time`, `test_id`) VALUES (2, 'test', 'password', NULL, 2);
INSERT INTO `t_user` (`id`, `name`, `password`, `create_time`, `test_id`) VALUES (13, 'test', 'password', NULL, 2);

```

### Building and running

#### Development

Start Postgres container:

Start FastGQL in dev mode with hot reload:

Go to [GraphQL Playground](https://github.com/graphql/graphql-playground) or query ```localhost:8888/graphql```

#### Production

Build production bundle:
