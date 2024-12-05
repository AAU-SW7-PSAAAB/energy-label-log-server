# energy-label-log-server

A logging server to log data from runs of the energy label extension.

# Setup

Make sure you have a MariaDB running. To setup the default user do

## MariaDB

To setup the default user do

```bash
$ sudo mariadb
```

```SQL
CREATE DATABASE energylabel;
```

```SQL
CREATE USER energylabel@localhost IDENTIFIED BY energylabel;
```

```SQL
GRANT ALL PRIVILEGES ON energylabel.* TO energylabel@localhost;

```

## Install program

```bash
$ git clone https://github.com/AAU-SW7-PSAAAB/energy-label-log-server/
```

```bash
$ npm i
```

## Setup initial database tables

```bash
$ npm run exec -- --mariadb-init
```

If column store is not installed do

```bash
$ npm run exec -- --mariadb-init --mariadb-column-store=false
```

To list cli arguments run

```bash
$ npm run exec -- --help
```

To run the server do

```bash
$ npm run exec -- <argslist>
```

## Lint

To lint and format the project do

```bash
$ npm run lint
```
