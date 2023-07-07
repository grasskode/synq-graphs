# Graph Lineage

Run tests:
```
go test
```

## Approach

> 💡 Considerations:
* Typical graph has up to 10M relationships
* Paths of assets could be rather long as they are formatted from underlying external storage out of our control. You can assume string length of up to 1,024.
* Optimise for query performance over graph load performance. The latency of the system for queries should be in milliseconds even for large lineages (10k nodes)
* Identifiers in source data are globally unique in Synq system
* Input data will not have any cycles (A → B, B → C, C → A)
* Input data could have duplicate relationships, so same pair of source,target nodes could repeat. In such case no new relationship should be created. Duplicate records can be simply ignored.

Since we want to optimize for query performance, we can start with graph represented by nodes that hold their upstream and downstream connections.

```golang
  // Graph stores the graph representation and exposes
  // the functions used to traverse lineage. It stores
  // the nodes mapped by their paths.
  type Graph struct {
  	nodes map[string]*Node
  }

  // Node represents a single node in the graph. It contains
  // the path of the node and the immediate upstream and
  // downstream relations.
  type Node struct {
  	path       string
  	upstream   []string
  	downstream []string
  }

  func (g *Graph) upstream(paths []string) ([]string, error) {
    // implement
  }

  func (g *Graph) downstream(paths []string) ([]string, error) {
    // implement
  }
```

We can use recursion to calculate upstream and downstream paths for any given list of incoming paths. Given this structure, these lookups should be available with `O(k)` complexity where `k` is the maximum depth of the graph. Worst case for a single file lineage, this corresponds to `O(n)` where `n` is the number of nodes in the graph. This gives us a linear performance. We could improve this further with caching if required (LRU or most time consuming paths).


* Why did you choose your design and what are its strengths and weaknesses?

  * I chose the design for its simplicity. The design caters to the problem at hand without losing information.
  * Operations (except insertion of edge) involving modification of graph might not be catered to in this representation.

* What trade-offs did you make for simplicity, speed of development, and other factors?

  * The map of nodes assumes that the the graph size is limited. Given that a typical graph contains 10M relations, the graph should fit in the memory. However, if we need to manage multiple graphs in the memory, this could be a problem.
  * We are assuming that the graph can be read multiple times after being loaded once. If the graph needs to be simultaneously written and read then we would need to add concurrent access control using mutex.
  * I was unable to get the parquet reader to work. I converted the parquet to a CSV and used that as the input.

* How have you or could you improve the resiliency of your solution?
  * The code currently assumes all the assumptions (no cycles, data types, etc.) provided in the assignment. These should be checked in the code instead and errors should be appropriately handled.
  * The code processes a node only once but uses a list to track the processed nodes. This could be moved to a map for better lookup performance.
  * We could possibly process multiple paths sent to upstream or downstream methods concurrently. This would mean that we would need to merge the results to de-duplicate but for longer inputs this could be a desirable trade off.
