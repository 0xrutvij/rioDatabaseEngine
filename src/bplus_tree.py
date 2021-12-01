from __future__ import annotations
import bisect
import random
from typing import Tuple, Union

from btree import DataPointer, Node
    

class BPlusNode(Node):
    def __init__(self, leaf: bool, parent: Node) -> None:
        super().__init__(leaf)
        self.parent: BPlusNode = parent
        self.next: BPlusNode = None
        self.prev: BPlusNode = None

class BPlusTree:

    def __init__(self, arity: int) -> None:
        BPlusNode.MIN_DEG = arity/2 if arity >= 6 else 3
        self.root = BPlusNode(True, None)

    def bptree_search(self, key: Union[DataPointer, int]):
        (node, idx) = self._search(self.root, key)
        if idx is None:
            raise KeyError(f"Key {key} not found in B+ Tree!")
        else:
            return (node, idx)

    def _search(self, node: BPlusNode, key: Union[DataPointer, int]) -> Tuple[BPlusNode, int]:
        i = 0
        n = len(node.keys)

        while i < n and key > node.keys[i]:
            i += 1

        if node.is_leaf:
            if i < n and key == node.keys[i]:
                return (node, i)
            else:
                return (node, None)
        else:
            return self._search(node.pointers[i], key)

    def bptree_insert(self, entry: Union[DataPointer, int]) -> None:
        insertion_leaf, _ = self._search(self.root, entry)
        max_key_fill = BPlusNode.max_ptr_degree() - 1

        if len(insertion_leaf.keys) >= max_key_fill:
            # split the leaf, recursively call insert on parent with copied key.
            (mkey, lc, rc) = self._insert_split_leaf(insertion_leaf, entry)
            if insertion_leaf.parent:
                self._insert_up(insertion_leaf.parent, mkey, lc)
            else:
                new_root = BPlusNode(False, None)
                new_root.keys = [mkey]
                new_root.pointers = [lc, rc]
                lc.parent = new_root
                rc.parent = new_root
                self.root = new_root

        else:
            bisect.insort_left(insertion_leaf.keys, entry)

    def _insert_up(self, parent: BPlusNode, router: int, lc: BPlusNode):
        max_key_fill = BPlusNode.max_ptr_degree() - 1

        if len(parent.keys) >= max_key_fill:
            # insert and split, propogate up recursively
            (mkey, lci, rci) = self._insert_split_internal(parent, router, lc)
            if parent.parent:
                self._insert_up(parent.parent, mkey, lci)
            else:
                new_root = BPlusNode(False, None)
                new_root.keys = [mkey]
                new_root.pointers = [lci, rci]
                lci.parent = new_root
                rci.parent = new_root
                self.root = new_root
        else:
            i = bisect.bisect_left(parent.keys, router)
            parent.pointers.insert(i, lc)
            parent.keys.insert(i, router)

    def _insert_split_internal(self, internal_node: BPlusNode, router: int, lc: BPlusNode):
        index = bisect.bisect_left(internal_node.keys, router)
        internal_node.pointers.insert(index, lc)
        internal_node.keys.insert(index, router)

        split_node = BPlusNode(False, internal_node.parent)
        t = BPlusNode.min_ptr_degree()

        median_key = internal_node.keys[t+1]
        split_node.keys = internal_node.keys[:t+1]
        split_node.pointers = internal_node.pointers[:t+2]

        for ptr in split_node.pointers:
            ptr.parent = split_node

        internal_node.keys = internal_node.keys[t+2:]
        internal_node.pointers = internal_node.pointers[t+2:]

        return (median_key, split_node, internal_node)


    def _insert_split_leaf(self, leaf_node: BPlusNode, entry: Union[DataPointer, int]) -> Tuple[int, BPlusNode, BPlusNode]:
        bisect.insort_left(leaf_node.keys, entry)

        split_node = BPlusNode(True, leaf_node.parent)
        t = BPlusNode.min_ptr_degree()

        median_key = leaf_node.keys[t]
        split_node.keys = leaf_node.keys[:t+1]
        leaf_node.keys = leaf_node.keys[t+1:]

        if leaf_node.prev:
            leaf_node.prev.next = split_node

        split_node.prev = leaf_node.prev
        split_node.next = leaf_node
        leaf_node.prev = split_node

        return (median_key, split_node, leaf_node)
        

   
    def show(self, node: BPlusNode, _prefix="", _last=True):

        if node.is_leaf:
            print(_prefix, "`- " if _last else "|- ", str(node.keys), sep="")

        else:
            print(_prefix, "`- " if _last else "|- ", str(node.keys), sep="")
            _prefix += "   " if _last else "|  "
            for i, child in enumerate(node.pointers):
                _last = (i == len(node.pointers) - 1)
                self.show(child, _prefix, _last)


    def show_tree(self):
        self.show(self.root)


    
                
if __name__ == "__main__":
    tree = BPlusTree(6)

    random.seed(1)

    ls_nums = list(range(20))
    random.shuffle(ls_nums)

    for i in ls_nums:
        tree.bptree_insert(i)
    
    tree.show_tree()

    n, i = tree.bptree_search(19)

    while n:
        print(n.keys)
        n = n.prev
    

    