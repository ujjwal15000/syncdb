# SyncDB

## Overview

SyncDB is a distributed database solution that combines the power of **RocksDB** and **Apache Helix** for efficient data transport, dynamic partitioning, and high scalability. Designed to support multi-tenant clusters, SyncDB ensures optimal performance in modern, distributed systems while maintaining fault tolerance and high throughput.

## Key Features

- **High-Speed Data Transport**: Built on **Vert.x** for fast and reliable data movement, SyncDB ensures low latency and high throughput.
- **Dynamic Partitioning with Apache Helix**: Leverages **Helix** for automated partition orchestration, enabling efficient distribution of workloads across multiple nodes.
- **Multitenancy Support**: Fully isolated and multi-tenant clusters with dynamic partitioning for optimized resource utilization.
- **Custom Binary TCP Protocol**: A high-performance, custom binary TCP protocol with zero-copy optimizations, improving data transfer efficiency.
- **Flexible Filesystem Support**: SyncDB supports distributed filesystems, with the ability to extend to local filesystems, offering flexibility for varied deployment environments.
- **Built-in Replication**: Native support for replication ensures data availability and fault tolerance in distributed deployments.
- **Tablet-based Architecture**: Uses a tablet-based system that supports one writer and multiple readers per tablet for improved scalability and fault tolerance.

## Binary Protocol Specification

SyncDB uses a custom binary TCP protocol for communication between nodes. This protocol is designed to be efficient and lightweight, supporting high-throughput data exchange with minimal overhead.

```code
+---------+---------+------------+------------+------------------+---------+
| Header  |     Seq |   Msg Type | Payload Len|          Payload |     CRC |
| 2 Bytes | 4 Bytes |     1 Byte |    4 Bytes |  Variable Length | 2 Bytes |
+---------+---------+------------+------------+------------------+---------+
```
- Header: Fixed value to identify the start of a packet.
- Seq: Sequence number for message tracking.
- Msg Type: Message type identifier (e.g., request, response).
- Payload Len: Length of the payload in bytes.
- Payload: Actual message content (variable length).
- CRC: CRC16 checksum for data integrity.

## Architecture

SyncDB is designed with an advanced tablet-based architecture that divides data into logical units (tablets). Each tablet supports one writer and multiple readers, ensuring efficient data handling while maintaining high availability. The partitioning system is managed by **Apache Helix**, ensuring dynamic and fault-tolerant distribution of data across nodes.

## Installation

To get started with SyncDB, clone the repository and follow the setup instructions below:

```code
git clone https://github.com/ujjwal15000/syncdb.git
cd syncdb
mvn clean install
```
