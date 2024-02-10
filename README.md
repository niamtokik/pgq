# pgq

An Erlang library to use [pgq](https://pgq.github.io/extension/pgq)
postgresql extension.

## Build

```erlang
rebar3 compile
```

## Installation and Boostrap

Download and install pgq postgresql extension.

```sh
git clone https://github.com/pgq/pgq
make 
make install
```

Create a new database (or use an existing one) and enable the
extension.

```sql
CREATE DATABASE my_database;
\c my_database
CREATE EXTENSION pgq;
```

## Usage

## References and Resources

[pgq official documentation](https://pgq.github.io/extension/pgq)

[pgq official repository](https://github.com/pgq/pgq)
