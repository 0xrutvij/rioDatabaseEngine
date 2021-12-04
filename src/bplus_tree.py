from __future__ import annotations
import bisect
import copy
from os import stat
import random
import traceback
from typing import List, Tuple, Union

from btree import DataPointer, Node
#sys.stdout = open('file', 'w')

class BPlusNode(Node):
    def __init__(self, leaf: bool, parent: Node) -> None:
        super().__init__(leaf)
        self.pointers: List[BPlusNode] = []
        self.parent: BPlusNode = parent
        self.next: BPlusNode = None
        self.prev: BPlusNode = None

class BPlusTree:

    def __init__(self, min_ptr_degree: int = 3) -> None:
        self.root = BPlusNode(True, None)
        self.min_degree = max(min_ptr_degree, 3)


    def search(self, key: Union[DataPointer, int]):
        (node, idx) = self._search(self.root, key)
        if idx is None:
            raise KeyError(f"Key {key} not found in B+ Tree!")
        else:
            return (node, idx)

    def min_ptr_degree(self):
        return self.min_degree

    def max_ptr_degree(self):
        return 2 * self.min_degree

    def _search(self, node: BPlusNode, key: Union[DataPointer, int]) -> Tuple[BPlusNode, int]:
        i = 0
        n = len(node.keys)

        if node.is_leaf:
            while i < n and key > node.keys[i]:
                i += 1
            if i < n and key == node.keys[i]:
                return (node, i)
            else:
                return (node, None)
        else:
            while i < n and key >= node.keys[i]:
                i += 1
            return self._search(node.pointers[i], key)

    def insert(self, entry: Union[DataPointer, int]) -> None:
        insertion_leaf, _ = self._search(self.root, entry)
        max_key_fill = self.max_ptr_degree() - 1

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
        max_key_fill = self.max_ptr_degree() - 1

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
        t = self.min_ptr_degree()

        median_key = internal_node.keys[t]
        split_node.keys = internal_node.keys[:t]
        split_node.pointers = internal_node.pointers[:t+1]

        for ptr in split_node.pointers:
            ptr.parent = split_node

        internal_node.keys = internal_node.keys[t+1:]
        internal_node.pointers = internal_node.pointers[t+1:]

        return (median_key, split_node, internal_node)


    def _insert_split_leaf(self, leaf_node: BPlusNode, entry: Union[DataPointer, int]) -> Tuple[int, BPlusNode, BPlusNode]:
        bisect.insort_left(leaf_node.keys, entry)

        split_node = BPlusNode(True, leaf_node.parent)
        t = self.min_ptr_degree()

        median_key = leaf_node.keys[t]
        median_key = median_key.id if isinstance(median_key, DataPointer) else median_key
        split_node.keys = leaf_node.keys[:t]
        leaf_node.keys = leaf_node.keys[t:]

        if leaf_node.prev:
            leaf_node.prev.next = split_node

        split_node.prev = leaf_node.prev
        split_node.next = leaf_node
        leaf_node.prev = split_node

        return (median_key, split_node, leaf_node)
        
    def delete(self, key: int):
        val_loc, idx = self._search(self.root, key)

        if idx is None:
            print(val_loc.keys)
            raise KeyError(f"Key {key} not in tree.")

        val_loc.keys.remove(key)

        if self._node_is_underflow(val_loc) and val_loc.parent:
            vparent = val_loc.parent
            ptr_idx = vparent.pointers.index(val_loc)
            left_sib = vparent.pointers[ptr_idx - 1] if ptr_idx - 1 >= 0 else None
            right_sib = vparent.pointers[ptr_idx + 1] if ptr_idx + 1 < len(vparent.pointers) else None
            transfer_max = self.min_ptr_degree() + 1

            if right_sib and len(right_sib.keys) <= transfer_max:
                val_loc.keys += right_sib.keys
                vparent.pointers.pop(ptr_idx + 1)
                vparent.keys.pop(ptr_idx)

            elif left_sib and len(left_sib.keys) <= transfer_max:
                left_sib.keys += val_loc.keys
                vparent.pointers.pop(ptr_idx)
                vparent.keys.pop(ptr_idx - 1)

            elif right_sib and len(right_sib.keys) > transfer_max:

                while self._node_is_underflow(val_loc) and not self._node_is_underflow(right_sib):
                    pull_key = right_sib.keys.pop(0)
                    val_loc.keys.append(pull_key)
                    vparent.keys[ptr_idx] = right_sib.keys[0].id if isinstance(right_sib.keys[0], DataPointer) else right_sib.keys[0]

            elif left_sib and len(left_sib.keys) > transfer_max:

                while self._node_is_underflow(val_loc) and not self._node_is_underflow(left_sib):
                    pull_key = left_sib.keys.pop()
                    val_loc.keys.insert(0, pull_key)
                    vparent.keys[ptr_idx - 1] = pull_key.id if isinstance(pull_key, DataPointer) else pull_key

            if self._node_is_underflow(vparent):
                self._fuse_or_share_internal(vparent)
            else:
                return True
        else:
            return True


    def _fuse_or_share_internal(self, node: BPlusNode):

        if gp := node.parent:
            idx = gp.pointers.index(node)
            ls = gp.pointers[idx-1] if idx - 1 >= 0 else None
            rs = gp.pointers[idx+1] if idx + 1 < len(gp.pointers) else None
            transfer_max = self.min_ptr_degree() + 1

            if rs and len(rs.keys) <= transfer_max:
                median_key = gp.keys.pop(idx)
                gp.pointers.pop(idx+1)
                node.keys = node.keys + [median_key] + rs.keys
                for ptr in rs.pointers:
                    ptr.parent = node
                node.pointers.extend(rs.pointers)

            elif ls and len(ls.keys) <= transfer_max:
                median_key = gp.keys.pop(idx-1)
                gp.pointers.pop(idx)
                ls.keys = ls.keys + [median_key] + node.keys
                for ptr in node.pointers:
                    ptr.parent = ls
                ls.pointers.extend(node.pointers)

            elif rs and len(rs.keys) > transfer_max:

                while self._node_is_underflow(node) and not self._node_is_underflow(rs):
                    pull_ptr = rs.pointers.pop(0)
                    pull_key = rs.keys.pop(0)
                    node.keys.append(gp.keys[idx])
                    pull_ptr.parent = node
                    node.pointers.append(pull_ptr)
                    gp.keys[idx] = pull_key

            elif ls and len(ls.keys) > transfer_max:

                while self._node_is_underflow(node) and not self._node_is_underflow(ls):
                    pull_ptr = ls.pointers.pop()
                    pull_key = ls.keys.pop()
                    node.keys.insert(0, gp.keys[idx-1])
                    pull_ptr.parent = node
                    node.pointers.insert(0, pull_ptr)
                    gp.keys[idx-1] = pull_key

            if self._node_is_underflow(gp):
                self._fuse_or_share_internal(gp)

        elif len(node.keys) == 0:
            new_root = node.pointers.pop()
            new_root.parent = None
            self.root = new_root

    @staticmethod
    def _borrow_left_leaf(node: BPlusNode, left_sib: BPlusNode):
        if left_sib and BPlusTree._is_more_than_half(left_sib) and left_sib.is_leaf:
            new_key = left_sib.keys.pop()
            node.keys.insert(0, new_key)
            return True
        
        return False


    @staticmethod
    def _borrow_right_leaf(node: BPlusNode, right_sib: BPlusNode):
        if right_sib and BPlusTree._is_more_than_half(right_sib) and right_sib.is_leaf:
            new_key = right_sib.keys.pop(0)
            node.keys.append(new_key)
            return True
        
        return False

    @staticmethod
    def _merge_leaves(left: BPlusNode, right: BPlusNode):
        
        left.next = right.next
        if right.next:
            right.next.prev = left
        
        left.keys += right.keys
        return 

    def _node_is_underflow(self, node: BPlusNode):
        return len(node.keys) < self.min_ptr_degree()

    def _is_more_than_half(self, node: BPlusNode):
        return len(node.keys) > self.min_ptr_degree()


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
    tree = BPlusTree()

    random.seed(1)

    ls_nums = list(range(300000))
    random.shuffle(ls_nums)

    for i in ls_nums:
        dp = DataPointer(int.__int__, i)
        tree.insert(dp)
        # tree.show_tree()

    random.shuffle(ls_nums)
    """
    for i in ls_nums:
        try:
            if i == 3827 or i == 3814:
                tree.show_tree()
            tree.btree_delete(i)
        except Exception as e:
            print(f"\n\n\n Num is {i} \n")
            print(traceback.format_exc())
            tree.show_tree()
            break

    # tree.show_tree()
    """
    for i in ls_nums:
        #print(f"\n\nDeleting key {i}:")
        try:
            tree.delete(i)
        except:
            traceback.print_exc()
            tree.show_tree()
            break
        
    #tree.show_tree()