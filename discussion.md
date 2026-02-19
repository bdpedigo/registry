---
marp: true
theme: aibs
size: 16:9
paginate: true
math: true
backgroundImage: url(../themes/aibs-backgrounds/default.png)
transition: fade 0.1s
---

<script type="module">
  import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
  mermaid.initialize({ startOnLoad: true });
</script>

# Connectomics Feature Registry/Catalog (proposal) <!-- omit from toc -->

This service would provide a **metadata registry** for large, externally stored feature tables and embeddings (Parquet, Delta Lake, Iceberg, Lance, etc.) generated from connectomics data. It is designed to be a **lightweight, descriptive layer** that enables discovery, interpretation, and reuse of feature datasets without owning the data. It also aims to handle access so that users can easily retrieve data but access costs can be managed.

---

# Use Cases

*Potential* use cases for this registry include (we may drop some of these or add others as we refine the scope):

---

## Bulk

_Tasks along the lines of "just give me a large chunk of some data table"_

- Download the entire synapse table for a fixed materialization version for offline analysis

---

## Data analyst

_Tasks that involve gathering data into a clean table useful for exploration or making scientific plots, using information beyond what is currently supported in CAVE_

- Pull the synapse table for proofread axon cells, but with a extra columns showing distance from postsynaptic soma and bouton size, both of which were pre-computed and placed in feature tables for a fixed materialization version and registered in the registry
- Pull the raw soma-nucleus features for latest nuclei to make a plot of how nucleus folding varies between two cell types

---

## Data scientist

_Tasks that involve training models, deploying models, or doing computational search that might involve more complicated queries and joins_

- Pull SegCLR embeddings for presynaptic sites from proofread cells for training a model
- Query for SegCLR embeddings which are similar to a given synapse embedding (e.g. for searching for "synapses like me")
- Pull HKS features for pre- and post-synaptic sites for all synapses for a fixed materialization version
- Pull spine morphometry features for all spines that are within 500nm of the soma on excitatory cells

---

# Proposed design

- Datasets are stored in a bucket in object storage (GCS, S3, etc.) and are not copied or proxied by the registry
- A metadata registry service with a simple API to register datasets and query metadata
- Users can query the registry for metadata about available datasets to plan queries
- Users need to request temporary signed URLs from the registry to access the data, which allows for access control and cost management

---

# Registering a table

<div class="mermaid">
  sequenceDiagram;
  actor Producer as Data scientist / system;
  participant Storage as Object storage;
  participant Registry as Feature registry;
  Producer->>Producer: Generate table;
  Producer->>Storage: Write table;
  Producer->>Registry: Register table + metadata;
  Registry->>Registry: Validate metadata;
  Registry-->>Producer: Registration confirmation;
</div>

---

# Accessing a table

<div class="mermaid fixed-height" style="--mermaid-height: 500px;">
sequenceDiagram
  actor User
  participant Registry as Feature Registry (This service)
  participant Engine as Query Engine (Polars, DuckDB, etc.)
  participant Storage as Object Storage (GCS, S3, etc.)
  User->>Registry: Request table metadata
  Registry-->>User: Return table info + location
  User->>User: Plan query
  User->>Registry: Request temporary signed URL
  Registry-->>User: Signed URL
  User->>Engine: Execute query with signed URL
  Engine->>Storage: Read data via signed URL
  Storage-->>Engine: Data
  Engine-->>User: Query results
</div>

---

# Decision Points

---

## What metadata fields to use initially?

<div class="columns">
<div>

- table name
- description
- path to table
- format (Delta, Iceberg, Lance...)
- schema (column names and types), optional text description
- datastack or aligned volume identifier
- snapshot timestamp or time range
- materialization version alignment
- ownership (team, individual)
- invalidation radius

</div>
<div>

- licensing or usage constraints
- retention policy / TTL (if applicable)
- citation (if applicable)
- references to annotation tables and columns
- tags or categories
- information about the version of the pipeline/model that produced the data
- status (draft, published, deprecated)
- lineage (superseded by, derived from)
- partition columns / hash functions

</div>
</div>

---

## What to enforce vs just record as metadata?

- make sure the table exists at the given location
- auto-detect schema from the data?
- verify materialization version exists? what if it is not a long term release?
- how much validation to do on references to annotation tables and columns? just record as text or try to validate against CAVE?

---

## What to use as the underlying metadata platform?

- DataHub
  - [Blog on the creation of DataHub](https://www.linkedin.com/blog/engineering/data-management/datahub-popular-metadata-architectures-explained)
- OpenMetadata
  - [Blog on why to use OpenMetadata](https://blog.open-metadata.org/why-openmetadata-is-the-right-choice-for-you-59e329163cac) (I found it really jargony)
  - [Doc on main concepts in OpenMetadata](https://docs.open-metadata.org/latest/main-concepts/high-level-design?utm_source=google&utm_medium=paid-search&utm_campaign=omd-goog-amer-en-brand-aw&utm_adgroup=165976860866&utm_term=open%20metadata&utm_campaign=21627991159&utm_source=google&utm_medium=paid_search&hsa_acc=2097547741&hsa_cam=21627991159&hsa_grp=165976860866&hsa_ad=710804984805&hsa_src=g&hsa_tgt=kwd-903209659080&hsa_kw=open%20metadata&hsa_mt=e&hsa_net=adwords&hsa_ver=3&gad_source=1&gad_campaignid=21627991159&gbraid=0AAAAA9-MDDldmZedqY4SwsmfITe4iuQDB&gclid=Cj0KCQiA-YvMBhDtARIsAHZuUzLdz664JsOyzfaH_IrTSpA1XE3joAIBa9H-Sd8KJiqMWj3F9fK2dwIaAkB9EALw_wcB)
- Amundsen
- Unity Catalog
- [Marquez](https://marquezproject.ai/about/)
- Custom solution

---

## At what level to define access control?

- can any user access any table for the datastack?
- how to parameterize amount that someone can query?

---

## What is our contract with users regarding table availability and durability?

- future might involve worker systems which write data on some schedule or dynamically as features are updated (e.g. Dagster)
- annotation tables from CAVE on the other hand should probably remain static

---

## What CAVE-like interface (if any) to provide users?

---

## What is the mechanism for writing table dumps from materialization engine to object storage and registering them in the registry?

---

## Future

- This enables a future where systems are generating features on a schedule or on the fly in the interim, and registering them automatically with the registry.
- How to ensure to keep this compatible with future services which might involve worker systems which write data on some schedule or dynamically as features are updated (e.g. Dagster)?
