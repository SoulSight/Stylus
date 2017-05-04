Stylus: a Strongly-Typed Store for Serving Massive RDF Data
===========================================================

Stylus is a strongly-typed store for serving massive RDF data. It is built on top of a distributed in-memory key-value store. The most distinguishing characteristics of Stylus is its strongly-typed storage scheme for modeling RDF entities. 

[TOC]

## Design Philosophy
### Requirements

Stylus is built on top of a distributed in-memory key-value store that **1)** supports *in-place* data access to the selected parts of a data record, instead of serializing or deserializing the whole KV pair; **2)** supports *message passing* between distributed servers.

#### Why an In-memory Infrastructure

An efficient distributed in-memory key-value store is an essential part of Stylus. On the one hand, efficient parallel processing of large graphs requires an efficient storage infrastructure that supports fast random data
access of the graph data [1] and the main memory (RAM) is still the most viable approach to fast random access. On the other hand, the ever growing size of knowledge requires scalable solutions and distributed systems built using commodity servers are usually more economical and easier to maintain compared with scale-up approaches. Particularly, we build our RDF store on top of [Microsoft Trinity Graph Engine][https://www.graphengine.io/], which well meet the requirements discussed above.

#### Why a Strongly-Typed Storage Scheme
The benefits of the strongly typed storage scheme is providing **1)** a compact storage,  **2)** fast random data accesses, and **3)** reduced joins for query processing.

#### Why Previous Solutions Fail

The idea of defining entity types using grouped predicates has already been adopted in the RDBMS based models. However, they are failed to deliver high performance due to the following reasons. First, the multi-valued properties are hard to arrange in relational tables as faced by property table based methods. In this case, an individual table is necessary for storing them, but multi-valued properties spreading all over the data sets reduce this model back to a giant untyped triple table at performance; Second, real-life entities are likely to play multiple roles, meaning each entity may have combinatorial properties from very different aspects. A common practice is to span the entity’s data across several records. However, additional joins are inevitable for aggregating entity segments in this case. The more records those entities are partitioned, the more joins are needed for aggregation; Third, a fixed schema agnostic of the data set is likely to produce lots of NULLs for the absent properties of entities. It is very costly to store those unnecessary NULLs, especially for a large number of predicates.

#### Introduction to xUDT and xTwig
As most graph processing tasks are IO-intensive and Stylus uses RAM as its main storage, designing a compact and efficient storage scheme becomes one of the core problems.

The most important concept is the *User-Defined Types*, a.k.a. UDTs. 

![Architecture Overview of Stylus](.\res\Figures\xUDT_Illustration.png){:height="24px"}

The key data structure we designed for compact representation of the intermediate results is xTwig.

![Architecture Overview of Stylus](.\res\Figures\xTwig.png)

#### Distributed Execution

## System Architecture 

Stylus compacts the storage by replacing [RDF literals][https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal], which are used for values such as strings, numbers, and dates, by their integer IDs. Stylus keeps a literal-to-id mapping table that translates literals of a SPARQL query into ids during query processing and maps the ids back to literals before returning results.

Most importantly, Stylus always models an RDF data set as a strongly-typed directed graph. Each node in the graph represents a unique entity using a record with several data fields. A graph node corresponds to either an subject or an object of the RDF data set. The storage scheme adopted by Stylus is given as follows: Given an RDF data set, Stylus will scan the data, extract metadata, and build a data schema for the data set. The generated schema contains all the strongly-typed data types needed for describing the data set. Stylus then stores each entity in a single record for fast data access according to the data schema.

Stylus is distributed SPARQL query processing engine on top of the strongly-typed storage scheme given above. The overall architecture of Stylus is shown in the figure. 

![Architecture Overview of Stylus](.\res\Figures\ServingDesign.png)

The whole RDF graph is partitioned over a cluster of the servers using random hashing. Each server has duplicated graph schema, but the data partitions are disjoint. A user submits a query to the query coordinator. The coordinator generates a query plan based on prepared statistics and indices and distributes the query plan to all servers. Then, each server executes the query plan and send back the partial query results to the coordinator. On receiving all partial results, the coordinator aggregates them and return the final result to the user.

## Manual of Stylus

#### Requirements & Dependencies

Platform .Net 4.5

Graph Engine >= 1.0.8482

dotNetRDF >= 1.0.12

#### Data Preparation

#### Single-machine Mode

#### Distributed Mode

## Instruction for Usage

[1]: A. Lumsdaine, D. Gregor, B. Hendrickson, and J. Berry. **Challenges in parallel graph processing.** Parallel Processing Letters, 17(01), 2007.