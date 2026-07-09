"""Graph representation for workflow execution."""

from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional
from models.node import Node, OperationSignatureList


@dataclass
class GraphNode:
    """A node in the execution graph."""
    node: Node
    operation: OperationSignatureList
    next_nodes: List['GraphNode'] = field(default_factory=list)
    dependencies: List['GraphNode'] = field(default_factory=list)
    executed: bool = False
    
    @property
    def key(self) -> str:
        """Unique key for this graph node."""
        return f"{self.node.container_name}:{self.operation.operation_signature.operation_name}"
    
    def __hash__(self):
        """Make GraphNode hashable."""
        return hash(self.key)
    
    def __eq__(self, other):
        """Make GraphNode comparable."""
        if not isinstance(other, GraphNode):
            return False
        return self.key == other.key


@dataclass
class ExecutionGraph:
    """Directed graph for workflow execution."""
    start_nodes: List[GraphNode] = field(default_factory=list)
    all_nodes: Dict[str, GraphNode] = field(default_factory=dict)
    
    def add_node(self, graph_node: GraphNode):
        """Add a node to the graph."""
        self.all_nodes[graph_node.key] = graph_node
    
    def get_ready_nodes(self) -> List[GraphNode]:
        """Get nodes that are ready to execute (all dependencies completed)."""
        ready = []
        for node in self.all_nodes.values():
            if not node.executed and all(dep.executed for dep in node.dependencies):
                ready.append(node)
        return ready
    
    def is_complete(self) -> bool:
        """Check if all nodes have been executed."""
        return all(node.executed for node in self.all_nodes.values())
    
    def get_leaf_nodes(self) -> List[GraphNode]:
        """Get nodes with no next nodes (leaf nodes)."""
        return [node for node in self.all_nodes.values() if not node.next_nodes]