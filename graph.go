package graph

import (
	"encoding/csv"
	"fmt"
	"os"
)

// MissingNodeError is thrown when the graph cannot find
// a node for requested path.
type MissingNodeError struct {
	path string
}

func (m *MissingNodeError) Error() string {
	return fmt.Sprintf("missing node for path %s", m.path)
}

// Node represents a single node in the graph. It contains
// the path of the node and the immediate upstream and
// downstream relations.
type Node struct {
	path       string
	upstream   []string
	downstream []string
}

// Graph stores the graph representation and exposes
// the functions used to traverse lineage. It stores
// the nodes mapped by their paths.
type Graph struct {
	nodes map[string]*Node
}

// Gets all the upstream nodes in the graph for the given paths.
func (g *Graph) upstream(paths []string) ([]string, error) {
	found := make(map[string]bool)
	processed := []string{}
	for {
		if len(paths) == 0 {
			break
		}
		path := paths[0]
		paths = paths[1:]
		if contains(processed, path) {
			// skip path if it is already processed
			continue
		}
		node, ok := g.nodes[path]
		if !ok {
			return nil, &MissingNodeError{path: path}
		}
		// push node's upstream relations to process
		paths = append(paths, node.upstream...)
		// add upstreams to found
		for _, up := range node.upstream {
			found[up] = true
		}
		// mark path as processed
		processed = append(processed, path)
	}

	// return the keys of the found nodes
	result := make([]string, len(found))
	i := 0
	for k := range found {
		result[i] = k
		i++
	}
	return result, nil
}

// Gets all the downstream nodes in the graph for the given paths.
func (g *Graph) downstream(paths []string) ([]string, error) {
	found := make(map[string]bool)
	processed := []string{}
	for {
		if len(paths) == 0 {
			break
		}
		path := paths[0]
		paths = paths[1:]
		if contains(processed, path) {
			// skip path if it is already processed
			continue
		}
		node, ok := g.nodes[path]
		if !ok {
			return nil, &MissingNodeError{path: path}
		}
		// push node's downstream relations to process
		paths = append(paths, node.downstream...)
		// add downstreams to found
		for _, up := range node.downstream {
			found[up] = true
		}
		// mark path as processed
		processed = append(processed, path)
	}

	// return the keys of the found nodes
	result := make([]string, len(found))
	i := 0
	for k := range found {
		result[i] = k
		i++
	}
	return result, nil
}

// Returns the node corresponding to the path. Creates one
// if it does not exist.
func (g *Graph) getOrCreate(path string) *Node {
	node, ok := g.nodes[path]
	if !ok {
		node = &Node{
			path:       path,
			upstream:   []string{},
			downstream: []string{},
		}
		g.nodes[path] = node
	}
	return node
}

// Checks if the given slice contains a string.
func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

// Inserts the given relation to the graph.
func (g *Graph) insert(from string, to string) {
	if g.nodes == nil {
		g.nodes = make(map[string]*Node)
	}
	fromNode, toNode := g.getOrCreate(from), g.getOrCreate(to)
	if !contains(fromNode.downstream, to) {
		fromNode.downstream = append(fromNode.downstream, to)
	}
	if !contains(toNode.upstream, from) {
		toNode.upstream = append(toNode.upstream, from)
	}
}

// Print the graph nodes. Used for debugging.
func (g *Graph) print() {
	for _, node := range g.nodes {
		fmt.Println(node.path, "-> upstream:", node.upstream, "downstream:", node.downstream)
	}
}

// NewGraphFromParquet reads input parquet file and greates a graph from
// the given relationships.
func NewGraphFromParquet(path string) (*Graph, error) {
	skip, limit := 0, 1000
	graph := &Graph{}
	for {
		records, err := ReadParquet(path, skip, limit)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			graph.insert(record.source, record.target)
		}
		skip += limit
	}
	return graph, nil
}

// NewGraphFromCsv reads input CSV file and greates a graph from
// the given relationships.
func NewGraphFromCsv(path string) (*Graph, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	graph := &Graph{}
	for _, record := range records[1:] {
		graph.insert(record[0], record[1])
	}
	return graph, nil
}
