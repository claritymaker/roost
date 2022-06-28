# Overview

Roost is a tool to help with collecting data to generate tabular data sets.

Design features:

* Collect packets from arbitrary sources
  * Packets _should_ have a consistent schema but it is not required
    * If no schema is pre-defined, the schema of the saved data is evolved to fit new data
    * if the schema is pre-defined, a malformed packet can either be dropped or cause an error
  * Each packet is saved in memory as an Arrow table
* Derive new packets
  * Snapshots: Most recent packets
  * Mapped packets: Apply functions (e.g. conversions)
* Triggers
  * On packet received
  * On batch
  * On finalize
* Rules
  * Save Data (csv, feather, parquet)
* Slice data

# API

The heart of Roost is the Storage object.

* Storage.add(packet_name, packet) => Add a packet. 
* Storage.batch(packet_name=None) => Converts all un-batched packets to a RecordBatch
* Storage.finalize() => Prevents addition of new packets. Triggers finalize rules (e.g. write data)
* Storage.add_rule(rule) => Add a rule about how to deal with data

Rules are instructions for how Storage should operate. There are canned rules

* Canned Rules
  * map
  * filter
* Generic Rule
  * Rule(triggers, actions)
    * triggers: list of triggers (on_add, on_batch, on_finalize, callable)
    * actions: list of callables
* Storage.generate(include, exclude, mappings) => Generate a new packet

# Future Plans

* Query data

