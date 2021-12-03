from __future__ import annotations
from functools import total_ordering
from typing import Callable, Generic, List, Tuple, TypeVar, Union
import random
T = TypeVar("T")


@total_ordering
class DataPointer(Generic[T]):
    
    def __init__(self, type_id_extractor: Callable[[T], int], keyed_instance: T) -> None:
        self.id: int = type_id_extractor(keyed_instance)
        self.id_extractor: Callable[[T], int] = type_id_extractor
        self.data: T = keyed_instance

    def __lt__(self, other: Union[DataPointer[T], int]) -> bool:
        if isinstance(other, int):
            return self.id < other
        return self.id < other.id

    def __eq__(self, other: Union[DataPointer[T], int]) -> bool:
        if isinstance(other, int):
            return self.id == other
        return self.id == other.id

    def __repr__(self) -> str:
        try:
            data_repr = str(self.data)
        except:
            data_repr = "..."
        finally:
            if len(data_repr) > 10:
                data_repr = "..."

        return (
            f"[ID: {self.id} DATA: {data_repr} ]"
        )


class Node:
    MIN_DEG = 3

    def __init__(self, leaf: bool) -> None:
        self.keys: List[Union[DataPointer, int]] = []
        self.pointers: List[Node] = []
        self.is_leaf = leaf
        
    @classmethod
    def min_ptr_degree(cls):
        return int(cls.MIN_DEG)

    @classmethod
    def max_ptr_degree(cls):
        return int(2 * cls.MIN_DEG)


class BTree:

    def __init__(self) -> None:
        self.root = Node(True)
        
    def search(self, key: Union[DataPointer, int]):
        return self._search(self.root, key)

    def _search(self, node: Node, key: Union[DataPointer, int]) -> Tuple[Node, int]:
        i = 0
        n = len(node.keys)

        while i < n and key > node.keys[i]:
            i += 1

        if i < n and key == node.keys[i]:
            return (node, i)
        elif node.is_leaf:
            return (None, None)
        else:
            return self._search(node.pointers[i], key)

    def insert(self, key: Union[DataPointer, int]):
        current_root = self.root

        if len(current_root.keys) == Node.max_ptr_degree() - 1:
            new_node = Node(False)
            self.root = new_node
            new_node.pointers.append(current_root)
            self._split_child(new_node, 0)
            self._insert_non_full(new_node, key)

        else:
            self._insert_non_full(current_root, key)

    def _split_child(self, node: Node, ptr_idx: int):
        
        node_to_split = node.pointers[ptr_idx]
        new_node = Node(node_to_split.is_leaf)

        t = Node.min_ptr_degree()

        new_node.keys = node_to_split.keys[t:]        
        new_node.pointers = node_to_split.pointers[t:]

        median_key = node_to_split.keys[t-1]

        node_to_split.keys = node_to_split.keys[:t-1]
        node_to_split.pointers = node_to_split.pointers[:t]

        node.keys = node.keys[:ptr_idx] + [median_key] + node.keys[ptr_idx:]
        node.pointers = node.pointers[:ptr_idx+1] + [new_node] + node.pointers[ptr_idx+1:]


    def _insert_non_full(self, node: Node, key: Union[DataPointer, int]):
        i = 0
        n = len(node.keys)

        while i < n and key > node.keys[i]:
                i += 1

        if node.is_leaf:
            node.keys = node.keys[:i] + [key] + node.keys[i:]

        else:
            insertion_ptr = node.pointers[i]

            if len(insertion_ptr.keys) == Node.max_ptr_degree() - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            
            self._insert_non_full(node.pointers[i], key)

    def delete(self, key: Union[DataPointer, int]):
        self._delete(self.root, key)
        if len(self.root.keys) == 0 and not self.root.is_leaf:
            self.root = self.root.pointers.pop()

    def _delete(self, node: Node, key: Union[DataPointer, int]):

        i = 0
        n = len(node.keys)
        while i < n and key > node.keys[i]:
            i += 1

        # case 0 and case 1 ~ Leaf node
        if node.is_leaf:
            # case 1
            if i < n and key == node.keys[i]:
                node.keys.pop(i)
                return True
            # case 0
            else:
                print(node.keys)
                raise KeyError(f"Key {key} not found in B Tree!")
        # case 2, 3 or 4 ~ Internal Node
        else:
            # case 2 ~ Key found in current node
            if i < n and key == node.keys[i]:

                left_child = node.pointers[i]
                right_child = node.pointers[i+1]

                # case 2(a) ~ left child has fill to supply replacement
                if self._check_fill(left_child):
                    replacement_key = self._find_left_predecessor_val(left_child)
                    node.keys[i] = replacement_key
                    return self._delete(left_child, replacement_key)
                # case 2(b) ~ right child has fill to supply replacement
                elif self._check_fill(right_child):
                    replacement_key = self._find_right_successor_val(right_child)
                    node.keys[i] = replacement_key
                    return self._delete(right_child, replacement_key)
                # case 2(c) ~ left and right children are sparse --> merge them.
                else:
                    # pop right pointer
                    ptr_to_merge = node.pointers.pop(i+1)
                    # pop matched key
                    matched_key = node.keys.pop(i)
                    # merge right pointer and matched key into left pointer
                    self._merge(left_child, matched_key, ptr_to_merge)
                    # call delete key on newly merged node
                    return self._delete(left_child, key)
            # case 4 
            elif self._check_fill(recurse_node := node.pointers[i]):
                return self._delete(recurse_node, key)
            # case 3 (recurse node is sparse)
            else:
                left_sibling = node.pointers[i-1] if i-1 >= 0 else None
                right_sibling = node.pointers[i+1] if i+1 < len(node.pointers) else None
                # case 3(a) - left sibling
                if left_sibling and self._check_fill(left_sibling):
                    # borrow from left sibling, supply to x
                    x_replacement = left_sibling.keys.pop()
                    x_donation = node.keys[i-1]
                    node.keys[i-1] = x_replacement
                    recurse_node.keys.insert(0, x_donation)
                    if len(left_sibling.pointers) > 0:
                        recurse_node.pointers.insert(0, left_sibling.pointers.pop())
                    return self._delete(recurse_node, key)
                # case 3(a) - right sibling
                elif right_sibling and self._check_fill(right_sibling):
                    # borrow from right sibling, supply to x
                    x_replacement = right_sibling.keys.pop(0)
                    x_donation = node.keys[i]
                    node.keys[i] = x_replacement
                    recurse_node.keys.append(x_donation)
                    if not right_sibling.is_leaf:
                        recurse_node.pointers.append(right_sibling.pointers.pop(0))
                    return self._delete(recurse_node, key)
                # case 3(c) - sparse siblings
                else:
                    # merge recurse node with one of the siblings.
                    # x_donation is the median key, lose a sibling pointer.
                    #prefer merging the right sibling
                    if right_sibling:
                        x_donation_final = node.keys.pop(i)
                        self._merge(recurse_node, x_donation_final, right_sibling)
                        node.pointers.pop(i+1)
                        return self._delete(recurse_node, key)
                    else:
                        x_donation_final = node.keys.pop(i-1)
                        self._merge(left_sibling, x_donation_final, recurse_node)
                        node.pointers.pop(i)
                        return self._delete(left_sibling, key)


    @staticmethod
    def _merge(left: Node, median_key: Union[DataPointer, int], right: Node):
        left.keys = left.keys + [median_key] + right.keys
        left.pointers.extend(right.pointers)

    @staticmethod
    def _check_fill(node: Node):
        return len(node.keys) >= Node.min_ptr_degree()


    @staticmethod
    def _find_left_predecessor_val(node: Node):
        if not node.is_leaf:
            return BTree._find_left_predecessor_val(node.pointers[-1])
        else:
            return node.keys[-1]

    @staticmethod
    def _find_right_successor_val(node: Node):
        if not node.is_leaf:
            return BTree._find_right_successor_val(node.pointers[0])
        else:
            return node.keys[0]

    def show(self):
        self._print_tree(self.root, prefix="", last=True)
        print("\n\n")

    def _print_tree(self, node: Node, prefix: str, last=False):

        print(prefix, "|- " if not last else "`- ", node.keys, sep="")

        if not node.is_leaf:
            for child in node.pointers[:-1]:
                self._print_tree(child, prefix+"  ", False)
            
            if node.pointers:
                self._print_tree(node.pointers[-1], prefix+"  ", True)
        

if __name__ == "__main__":

    tree = BTree()

    random.seed(1)

    ls_nums = list(range(30000))
    random.shuffle(ls_nums)

    for i in ls_nums:
        tree.insert(i)

    #tree.show()
    print("\n"*5)
    for num in ls_nums:
        tree.delete(num)
        
    tree.show()