**WorkingDB**

> **High-performance Toy multi-protocol database engine built in Rust**

**🚀 What is WorkingDB?**

WorkingDB is a blazing-fast, memory-optimized database that speaks multiple protocols (Redis, Memcached) so you can plug it into your existing stack without changing client code. Built on a zero-copy architecture with lock-free concurrency primitives, WorkingDB delivers microsecond-level latency while maintaining durability through its append-only file (AOF) persistence.

**Key Features:**
- 🔮 Multi-protocol support (Redis + Memcached compatibility)
- ⚡ Lock-free memory architecture for concurrent operations
- 💾 Automatic persistence with AOF journaling
- 🧵 Tokio-powered async I/O
- 🔄 Auto protocol detection
- ⏱️ Built-in TTL support for expiring keys
- 🧹 Background garbage collection

**🛠️ Getting Started**

**Installation**

```bash
# Clone the repo
git clone https://github.com/yourusername/workingdb.git
cd workingdb

# Build with optimizations
cargo build --release
```

**Running WorkingDB**

```bash
# Run with default settings (127.0.0.1:7777)
./target/release/workingdb

# Customize with environment variables
WORKINGDB_HOST=0.0.0.0 WORKINGDB_PORT=6380 WORKINGDB_DATA=/path/to/data ./target/release/workingdb
```

**💻 Client Connections**

Connect to WorkingDB using existing Redis or Memcached clients - the server auto-detects the protocol.

**Redis Example**

```bash
# Using redis-cli
redis-cli -h 127.0.0.1 -p 7777 SET mykey "Hello WorkingDB"
redis-cli -h 127.0.0.1 -p 7777 GET mykey

# From Python
import redis
r = redis.Redis(host='localhost', port=7777)
r.set('mykey', 'Hello WorkingDB')
value = r.get('mykey')
print(value)  # b'Hello WorkingDB'
```

**Memcached Example**

```bash
# Using memcached CLI
echo -e "set mykey 0 0 11\r\nHello World\r\n" | nc localhost 7777
echo -e "get mykey\r\n" | nc localhost 7777

# From Python
import pymemcache
client = pymemcache.Client(('localhost', 7777))
client.set('mykey', 'Hello WorkingDB')
value = client.get('mykey')
print(value)  # b'Hello WorkingDB'
```

**🔧 Configuration**

WorkingDB uses environment variables for configuration:

| Variable | Description | Default |
|----------|-------------|---------|
| `WORKINGDB_HOST` | Host to bind to | `127.0.0.1` |
| `WORKINGDB_PORT` | Port to listen on | `7777` |
| `WORKINGDB_DATA` | Data directory for persistence | `./data` |

**📋 Supported Commands**

**Redis Commands**
- `GET key` - Get the value of a key
- `SET key value [EX seconds]` - Set key to value with optional expiration
- `DEL key` - Delete a key
- `PING` - Test connection
- `INFO` - Server information

**Memcached Commands**
- `get <key>` - Get the value of a key
- `set <key> <flags> <exptime> <bytes> [noreply]` - Set key with optional expiration
- `delete <key> [noreply]` - Delete a key
- `stats` - Server statistics
- `version` - Server version

**🏗️ Architecture**

WorkingDB is built with a modular Rust architecture:

```
src/
├── core/       - Core database state management
├── storage/    - Memory and disk storage engines
├── network/    - Network protocol implementations
├── persistence/ - Durability and recovery
├── query/      - SQL query parsing and execution
└── util/       - Utility functions and helpers
```

**🔐 Data Persistence**

WorkingDB uses an append-only file (AOF) for durability. All write operations are logged to the AOF and replayed on startup to recover the in-memory state. No data loss, even in the event of a crash.

**🧪 Development**

```bash
# Run tests
cargo test

# Run with development features
cargo run --features dev
```

**📈 Performance**

WorkingDB is designed for high throughput and low latency:

- Memory-optimized storage with sharded hash tables
- Lock-free read paths for concurrent access
- Tokio async runtime for non-blocking I/O
- Zero-copy deserialization for network protocols

**🔮 Roadmap**

- [ ] SQL query engine
- [ ] Distributed clustering with Raft consensus
- [ ] Advanced compression for values
- [ ] RESP3 protocol support
- [ ] io_uring-based disk I/O
- [ ] Extended command set compatibility

**🤝 Contributing**

Contributions are welcome! Please feel free to submit a Pull Request.

**📄 License**

WorkingDB is licensed under the MIT License - see the LICENSE file for details.
