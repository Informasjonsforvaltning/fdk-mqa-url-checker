use sophia_api::source::TripleSource;
use sophia_api::term::SimpleTerm;
use sophia_isomorphism::isomorphic_graphs;
use sophia_turtle::parser::turtle::parse_str;

/// Assert that two Turtle RDF documents describe isomorphic graphs.
pub fn assert_isomorphic_turtle(actual: &str, expected: &str) {
    let actual_graph: Vec<[SimpleTerm; 3]> = parse_str(actual).collect_triples().unwrap();
    let expected_graph: Vec<[SimpleTerm; 3]> = parse_str(expected).collect_triples().unwrap();

    assert!(isomorphic_graphs(&expected_graph, &actual_graph).unwrap())
}
