package graph

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestInsert calls graph.insert to construct a graph and
// checks the constructed graph for expected structure.
func TestInsert(t *testing.T) {
	nodes := map[string][]string{
		"jaffle_shop.customers": []string{"stg_customers"},
		"jaffle_shop.orders":    []string{"stg_orders"},
		"stripe.payment":        []string{"stg_payments"},
		"gsheets.goals":         []string{"weekly_jaffle_metrics"},
		"stg_customers":         []string{"dim_customers"},
		"stg_orders":            []string{"dim_customers", "fct_orders"},
		"stg_payments":          []string{"fct_orders"},
		"dim_customers":         []string{"weekly_jaffle_metrics"},
		"fct_orders":            []string{"weekly_jaffle_metrics"},
	}
	graph := &Graph{}
	for path, downstreams := range nodes {
		for _, ds := range downstreams {
			graph.insert(path, ds)
		}
	}

	// assert number of nodes
	if len(graph.nodes) != 10 {
		t.Fatalf(`Node count mismatch. Expected %d, Found %d`, 10, len(graph.nodes))
	}

	// assert upstream and downstream relations for node
	// fct_orders ->
	//   upstream: [stg_orders stg_payments]
	//   downstream: [weekly_jaffle_metrics]
	node := graph.nodes["fct_orders"]
	sort.Strings(node.upstream)
	upstream := strings.Join(node.upstream, ",")
	expectedUpstream := "stg_orders,stg_payments"
	if upstream != expectedUpstream {
		t.Fatalf(`Upstream relations mismatch. Expected %v, Found %v`, expectedUpstream, upstream)
	}
	sort.Strings(node.downstream)
	downstream := strings.Join(node.downstream, ",")
	expectedDownstream := "weekly_jaffle_metrics"
	if downstream != expectedDownstream {
		t.Fatalf(`Downstream relations mismatch. Expected %v, Found %v`, expectedDownstream, downstream)
	}
}

// TestInsertWithDuplicateEntry calls graph.insert to construct a
// graph and checks the constructed graph for expected structure.
// Includes a duplicate entry that should not affect output.
func TestInsertWithDuplicateEntry(t *testing.T) {
	nodes := map[string][]string{
		"jaffle_shop.customers": []string{"stg_customers"},
		"jaffle_shop.orders":    []string{"stg_orders"},
		"stripe.payment":        []string{"stg_payments"},
		"gsheets.goals":         []string{"weekly_jaffle_metrics"},
		"stg_customers":         []string{"dim_customers"},
		"stg_orders":            []string{"dim_customers", "fct_orders", "fct_orders"},
		"stg_payments":          []string{"fct_orders"},
		"dim_customers":         []string{"weekly_jaffle_metrics"},
		"fct_orders":            []string{"weekly_jaffle_metrics"},
	}
	graph := &Graph{}
	for path, downstreams := range nodes {
		for _, ds := range downstreams {
			graph.insert(path, ds)
		}
	}

	// assert number of nodes
	if len(graph.nodes) != 10 {
		t.Fatalf(`Node count mismatch. Expected %d, Found %d`, 10, len(graph.nodes))
	}

	// assert upstream and downstream relations for node
	// fct_orders ->
	//   upstream: [stg_orders stg_payments]
	//   downstream: [weekly_jaffle_metrics]
	node := graph.nodes["fct_orders"]
	sort.Strings(node.upstream)
	upstream := strings.Join(node.upstream, ",")
	expectedUpstream := "stg_orders,stg_payments"
	if upstream != expectedUpstream {
		t.Fatalf(`Upstream relations mismatch. Expected %v, Found %v`, expectedUpstream, upstream)
	}
	sort.Strings(node.downstream)
	downstream := strings.Join(node.downstream, ",")
	expectedDownstream := "weekly_jaffle_metrics"
	if downstream != expectedDownstream {
		t.Fatalf(`Downstream relations mismatch. Expected %v, Found %v`, expectedDownstream, downstream)
	}
}

// TestParquet reads the parquet file and calls graph.insert to
// construct the graph for every record. Checks the constructed graph
// for expected structure.
// func TestParquet(t *testing.T) {
// 	filename := "synq-lineage.parquet"
// 	graph, err := NewGraphFromParquet(filename)
//  if err != nil {
//    t.Fatalf("Unable to read input file %s - %v", filename, err)
//  }
// 	// assert number of nodes
// 	if len(graph.nodes) != 266 {
// 		t.Fatalf(`Node count mismatch. Expected %d, Found %d`, 266, len(graph.nodes))
// 	}
// }

// TestCsv reads the CSV input file and calls graph.insert to
// construct the graph for every record. Checks the constructed graph
// for expected structure.
func TestCsv(t *testing.T) {
	filename := "synq-lineage.csv"
	graph, err := NewGraphFromCsv(filename)
	if err != nil {
		t.Fatalf("Unable to read input file %s - %v", filename, err)
	}

	// assert number of nodes
	if len(graph.nodes) != 266 {
		t.Fatalf(`Node count mismatch. Expected %d, Found %d`, 266, len(graph.nodes))
	}

	// Check node relations count for a given node.
	node := graph.nodes["dbt-sh-d577b364-a867-11ed-b4b2-fe8020e7ba25::model.ops.stg_runs"]
	if len(node.upstream) != 1 {
		t.Fatalf(`Upstream relations count mismatch. Expected %d, Found %d`, 1, len(node.upstream))
	}
	if len(node.downstream) != 6 {
		t.Fatalf(`Downstream relations count mismatch. Expected %d, Found %d`, 6, len(node.downstream))
	}
}

// TestUpstream asserts correct upstream output for basic graph.
func TestUpstream(t *testing.T) {
	nodes := map[string][]string{
		"jaffle_shop.customers": []string{"stg_customers"},
		"jaffle_shop.orders":    []string{"stg_orders"},
		"stripe.payment":        []string{"stg_payments"},
		"gsheets.goals":         []string{"weekly_jaffle_metrics"},
		"stg_customers":         []string{"dim_customers"},
		"stg_orders":            []string{"dim_customers", "fct_orders", "fct_orders"},
		"stg_payments":          []string{"fct_orders"},
		"dim_customers":         []string{"weekly_jaffle_metrics"},
		"fct_orders":            []string{"weekly_jaffle_metrics"},
	}
	graph := &Graph{}
	for path, downstreams := range nodes {
		for _, ds := range downstreams {
			graph.insert(path, ds)
		}
	}

	// Query: graph.upstream(stg_orders)
	// Result: [jaffle_shop.orders]
	upstream, err := graph.upstream([]string{"stg_orders"})
	if err != nil {
		t.Fatalf("Error getting upstream - %v", err)
	}
	sort.Strings(upstream)
	expected := []string{"jaffle_shop.orders"}
	sort.Strings(expected)
	if strings.Join(upstream, ",") != strings.Join(expected, ",") {
		t.Fatalf("Upstream mismatch. Expected %v, Found %v", expected, upstream)
	}

	// Query: graph.upstream(weekly_jaffle_metrics)
	// Result: [jaffle_shop.customers, jaffle_shop.orders, stripe.payment, stg_customers, stg_orders, stg_payments, dim_customers, fct_orders, gsheets.goals]
	upstream, err = graph.upstream([]string{"weekly_jaffle_metrics"})
	if err != nil {
		t.Fatalf("Error getting upstream - %v", err)
	}
	sort.Strings(upstream)
	expected = []string{"jaffle_shop.customers", "jaffle_shop.orders", "stripe.payment", "stg_customers", "stg_orders", "stg_payments", "dim_customers", "fct_orders", "gsheets.goals"}
	sort.Strings(expected)
	if strings.Join(upstream, ",") != strings.Join(expected, ",") {
		t.Fatalf("Upstream mismatch. Expected %v, Found %v", expected, upstream)
	}
}

// TestDownstream asserts correct downstream output for basic graph.
func TestDownstream(t *testing.T) {
	nodes := map[string][]string{
		"jaffle_shop.customers": []string{"stg_customers"},
		"jaffle_shop.orders":    []string{"stg_orders"},
		"stripe.payment":        []string{"stg_payments"},
		"gsheets.goals":         []string{"weekly_jaffle_metrics"},
		"stg_customers":         []string{"dim_customers"},
		"stg_orders":            []string{"dim_customers", "fct_orders", "fct_orders"},
		"stg_payments":          []string{"fct_orders"},
		"dim_customers":         []string{"weekly_jaffle_metrics"},
		"fct_orders":            []string{"weekly_jaffle_metrics"},
	}
	graph := &Graph{}
	for path, downstreams := range nodes {
		for _, ds := range downstreams {
			graph.insert(path, ds)
		}
	}

	// Query: graph.downstream(stg_orders)
	// Result: [dim_customers, fct_orders, weekly_jaffle_metrics]
	downstream, err := graph.downstream([]string{"stg_orders"})
	if err != nil {
		t.Fatalf("Error getting downstream - %v", err)
	}
	sort.Strings(downstream)
	expected := []string{"dim_customers", "fct_orders", "weekly_jaffle_metrics"}
	sort.Strings(expected)
	if strings.Join(downstream, ",") != strings.Join(expected, ",") {
		t.Fatalf("Downstream mismatch. Expected %v, Found %v", expected, downstream)
	}

	// Query: graph.downstream(weekly_jaffle_metrics)
	// Result: []
	downstream, err = graph.downstream([]string{"weekly_jaffle_metrics"})
	if err != nil {
		t.Fatalf("Error getting downstream - %v", err)
	}
	sort.Strings(downstream)
	expected = []string{}
	sort.Strings(expected)
	if strings.Join(downstream, ",") != strings.Join(expected, ",") {
		t.Fatalf("Downstream mismatch. Expected %v, Found %v", expected, downstream)
	}

	// Query: graph.downstream([stg_customers, stg_payments])
	// Result: [fct_orders, dim_customers, weekly_jaffle_metrics]
	downstream, err = graph.downstream([]string{"stg_customers", "stg_payments"})
	if err != nil {
		t.Fatalf("Error getting downstream - %v", err)
	}
	sort.Strings(downstream)
	expected = []string{"fct_orders", "dim_customers", "weekly_jaffle_metrics"}
	sort.Strings(expected)
	if strings.Join(downstream, ",") != strings.Join(expected, ",") {
		t.Fatalf("Downstream mismatch. Expected %v, Found %v", expected, downstream)
	}
}

// TestLoad checks runtime for huge graphs
func TestLoad(t *testing.T) {
	graph := &Graph{}
	for i := 0; i < 10000; i++ {
		graph.insert(strconv.Itoa(i), strconv.Itoa(i+1))
	}

	start := time.Now().UnixNano()
	downstream, err := graph.downstream([]string{"0"})
	end := time.Now().UnixNano()
	if err != nil {
		t.Fatalf("Error getting downstream - %v", err)
	}
	if len(downstream) != 10000 {
		t.Fatalf("Downstream count mismatch. Expected %v, Found %v", 10000, len(downstream))
	}
	fmt.Println("Fetched downstream in", (end-start)/1e6, "ms.")
}
