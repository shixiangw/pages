# Introducing Fleets: Modern IoT Device Management at Scale

Managing IoT devices at scale shouldn't be complicated. **Fleets** is an open-source IoT device management platform that simplifies how you register, monitor, and control your connected devices—whether you have ten devices or ten thousand.

Built on top of EMQX MQTT broker and designed for production workloads, Fleets provides a complete toolkit for device lifecycle management with a clean REST API and automatic OpenAPI documentation.

---

## Core Features

### 1. Device Registry with Thing Types

Fleets uses a **Thing Type** model where you define device schemas once and apply them across your fleet. A Thing Type specifies:

- **Properties** — device state attributes (temperature, battery level, lock state)
- **Events** — operational notifications with severity levels (info, warn, error)
- **Commands** — actions the cloud can invoke (sync for real-time, async for job-based)
- **Connectivity** — MQTT/HTTP protocols, TLS settings, keep-alive intervals

![Creating a Thing Type](./assets/fleets-new-type.png)

Once defined, **Things** (device instances) automatically inherit their type's schema. This eliminates repetitive configuration and ensures consistency across similar devices.

![Registering a Thing](./assets/fleets-register-thing-edit.png)

---

### 2. Device Shadow: The Single Source of Truth

The **Device Shadow** is Fleets' most powerful feature for state management. It maintains a three-state model:

| State | Direction | Purpose |
|-------|-----------|---------|
| **reported** | Device → Cloud | What the device reports as its current state |
| **desired** | Cloud → Device | What the cloud wants the device to become |
| **delta** | Auto-computed | The difference between desired and reported |

![Shadow Sync Interface](./assets/fleets-shadow-sync.png)

Here's how it works:
1. Cloud updates the `desired` state via API
2. Fleets computes the delta (desired \ reported)
3. Delta is pushed to the device via MQTT
4. Device converges and reports back its new state
5. When delta becomes empty, the device is fully synchronized

This pattern enables reliable offline operation—devices sync their shadow when reconnecting, ensuring no state changes are lost.

---

### 3. Powerful Device Query

Finding specific devices in a large fleet is effortless with Fleets' **SQL-like query language**. Search by metadata, connection status, or shadow properties using familiar operators:

```
# Find online thermostats reporting temperature > 25°C
thingTypeName:Thermostat AND temperature>25 AND status:online

# Find devices with pending configuration updates
hasDelta:true AND connected:true

# Find offline devices within the last hour
NOT status:online WITHIN LAST 1h
```

![Device Query Interface](./assets/fleets-device-query.png)

Queries support numeric comparisons, boolean logic, existence checks, and time-window filtering—making it easy to segment your fleet for targeted operations.

---

### 4. Commands & Jobs

Fleets provides two mechanisms for device control:

**Commands** — Real-time, request-response interactions with timeout handling. Perfect for immediate actions like "lock the door" or "reboot now."

![Sending a Command](./assets/fleets-command-send.png)

**Jobs** — Fleet-wide operations dispatched to multiple devices. Jobs support:
- Snapshot (one-time) or Continuous (ongoing) execution modes
- Per-thing execution tracking with status updates
- Automatic notification to devices when jobs are pending

![Creating a Job](./assets/fleets-create-job.png)

The Jobs protocol handles the complexity of unreliable networks—devices can query pending jobs, start the next available one, and report execution progress.

---

### 5. Tag-Driven Dynamic Groups

Organize devices with **tags** that enable dynamic group membership:

- Tag devices with attributes like `floor-1`, `temperature`, `production`
- Create **Thing Groups** with tag filters (e.g., all devices with both `HVAC` AND `floor-1` tags)
- Membership updates automatically when tags change

![Creating a Thing Group](./assets/fleets-new-group.png)

This approach scales better than manual group management—devices automatically join or leave groups based on their current tags.

---

### 6. Flexible Data Architecture

Fleets separates data by access patterns:

| Data Type | Storage | Use Case |
|-----------|---------|----------|
| Metadata, Shadow (desired/delta) | PostgreSQL | Fast lookups, transactional integrity |
| Shadow history, Events | EMQX Tables | Time-series queries, historical analysis |
| Optional high-frequency telemetry | EMQX Tables (custom topics) | High-volume numeric samples |

Devices can connect via **MQTT** (for real-time bidirectional) or **HTTPS** (for stateless uploads)—whatever fits your deployment constraints.

---

## Key Benefits

### ⚡ **Native EMQX Integration**
Fleets is built by the core EMQX product team — the people behind the world's most popular open-source MQTT broker. This means first-class, deeply optimized integration with EMQX's broker ecosystem: rule-engine bridging for events and telemetry, topic-based command routing under the `$emqx/` namespace, and lifecycle event hooks that keep your device state accurate. No third-party abstractions, no impedance mismatch — just the broker and platform working as one.

### 🔗 **End-to-End IoT Stack**
Fleets is the device management layer in EMQX's complete IoT value chain:

> **MQTT Device Connectivity** → **Time-Series Storage** → **Device Management Platform** → **Agent Automation**

EMQX handles massive-scale MQTT ingress. EMQX Tables stores telemetry and event history. Fleets manages the full device lifecycle — registry, shadow, commands, jobs. And on top, the **Agent automation** layer lets you build business workflows that respond to real-time device events. Every link in the chain is first-party, eliminating integration gaps that plague multi-vendor IoT stacks.

### ☁️ **Cloud-Agnostic Deployment**
No vendor lock-in. Fleets runs wherever you do — **AWS, Azure, GCP**, or on-premises — using standard, portable infrastructure (PostgreSQL, Docker). You control your data, your networking, and your compliance posture. Migrate between clouds or scale across regions without rewriting your device management layer.

### 🚀 **Production-Ready Architecture**
- Built with Go 1.23+ for performance and reliability
- PostgreSQL + EMQX Tables for scalable data storage
- EMQX V5 REST API for MQTT publishing—no client library complexity
- Horizontal scaling support with advisory locks for consistency

### 🔧 **Developer-Friendly**
- Auto-generated OpenAPI 3.1 documentation at `/docs`
- Dual authentication: Basic Auth + SignKey JWT
- Comprehensive API for all operations
- Clear MQTT topic conventions under `$emqx/` namespace

### 📊 **Observable and Queryable**
- Real-time device status and connection tracking
- SQL-like query language for fleet segmentation
- Time-series data storage for historical analysis
- Event severity classification (info, warn, error)

### 🏷️ **Flexible Organization**
- Tag-driven dynamic groups
- Thing Type inheritance for schema reuse
- Batch import/export for device migrations
- Standalone tags for Things and ThingTypes

### 🔒 **Security-First**
- TLS/mTLS support for device connections
- API key management with granular permissions
- SignKey JWT for stateless authentication
- Certificate-based device identity mapping

---

## Quick Start

Get Fleets running locally in minutes:

```bash
# Start infrastructure (PostgreSQL, EMQX, EMQX Tables)
make docker-up

# Run database migrations
DATABASE_URL="postgres://postgres:postgres@localhost:5432/fleets?sslmode=disable" make migrate-up

# Configure EMQX rules
make setup-emqx-rules

# Start Fleets
make run
```

Server starts at `http://localhost:8080` with API documentation at `/docs`.

---

## Deployment Overview

Fleets integrates seamlessly with your existing infrastructure:

![Deployment Overview](./assets/fleets-overview.png)

The platform connects to EMQX Broker for MQTT messaging and EMQX Tables for time-series data, while keeping metadata and state management in PostgreSQL.

---

## Use Cases

Fleets is ideal for:

- **Smart Building Management** — Control HVAC, lighting, and access systems across facilities
- **Industrial IoT** — Monitor sensors, manage gateways, and dispatch firmware updates
- **Connected Products** — Manage consumer devices with shadow state for seamless UX
- **Asset Tracking** — Query and group devices by location, status, or custom attributes

---

## Summary

Fleets brings together the best practices of IoT device management into a cohesive, production-ready platform. With its powerful Device Shadow for state synchronization, flexible query language for fleet operations, and robust command/job system for device control, Fleets simplifies the complexity of managing devices at scale.

Whether you're building a smart building platform, industrial monitoring system, or consumer IoT product, Fleets provides the foundation you need to focus on your application logic—not infrastructure plumbing.

**Learn more:** [github.com/emqx/fleets](https://github.com/emqx/fleets)  
**License:** MIT
