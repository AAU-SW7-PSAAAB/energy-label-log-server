# energy-label-log-server

A loggin server to log data from runs of the energy label extention.

# Setup
```
$ git clone https://github.com/AAU-SW7-PSAAAB/energy-label-log-server/
```

```
$ npm i
```

To list cli arguments run
```
$ npm run exec -- --help
```

To run the server do
```
$ npm run exec -- <argslist>
```

## Setup initial database tables
```
$ npm run exec -- --mariadb-init
```

## Lint
To lint and format the project do
```
$ npm run lint
```

