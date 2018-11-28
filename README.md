# TronEvents

An application server to save and retrieve events on the Tron blockchain.

## Notes on the architecture

TronEvents is composed by an http server and a few tasks.

The first
- receives info about new events emitted in the Tron blockchain and saves them
- retrieves info about events and provides dApp with the data

The seconds
- manage the Redis cache and the PostgreSQL database


#### How the data are saved

In the first version, every time an event is emitted, the event node (a special branch of java-tron) calls TronEvents, which saves the new event in Redis making it immediately available to dApps.

In the second version (coming later) an async task parses the Tron blockchain and as soon as a new event is emitted saves it in Redis.

In both cases any minute (or so), an async task saves the data in a PostgreSQL database, for durability and history.

The TronEvents http api provides dApps with the events emitted. It will require a poll initially, but it will use web socket for a better management.






