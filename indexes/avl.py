class AVLNode():
    def __init__(self, key, value):
        self.key = key
        self.values = [value]
        self.left = None
        self.right = None
        self.parent = None
        self.height = 1

    def __str__(self) -> str:
        return f"{str(self.key)} : {str(self.values)}"


class AVLTreeIndex():
    def __init__(self, name):
        self.name = name
        self.root = None
    
    def insert(self, key, value):
        if self.root is None:
            self.root = AVLNode(key, value)
        else:
            self._insert(key, value, self.root)

    def _insert(self, key, value, current_node):
        if key < current_node.key:
            if current_node.left is None:
                current_node.left = AVLNode(key, value)
                current_node.left.parent = current_node
                self._avl_insertion(current_node.left)
            else:
                self._insert(key, value, current_node.left)
        elif key > current_node.key:
            if current_node.right is None:
                current_node.right = AVLNode(key, value)
                current_node.right.parent = current_node
                self._avl_insertion(current_node.right)
            else:
                self._insert(key, value, current_node.right)
        else:
            current_node.values.append(value)

    def find(self, key):
        if self.root:
            return self._find(key, self.root)
        else:
            return None
    
    def _find(self, key, current_node):
        if not current_node:
            return None
        elif key == current_node.key:
            return current_node
        elif key < current_node.key:
            return self._find(key, current_node.left)
        else:
            return self._find(key, current_node.right)

    def _avl_insertion(self, node):
        if node.parent is None:
            return
        elif self._get_balance(node.parent) > 1 or self._get_balance(node.parent) < -1:
            self._rebalance(node.parent)
            return
        else:
            self._avl_insertion(node.parent)

    def _rebalance(self, node):
        if self._get_balance(node) < 0:
            if self._get_balance(node.right) > 0:
                self._rotate_right(node.right)
                self._rotate_left(node)
            else:
                self._rotate_left(node)
        elif self._get_balance(node) > 0:
            if self._get_balance(node.left) < 0:
                self._rotate_left(node.left)
                self._rotate_right(node)
            else:
                self._rotate_right(node)

    def _rotate_right(self, node):
        new_root = node.left
        node.left = new_root.right
        if new_root.right is not None:
            new_root.right.parent = node
        new_root.parent = node.parent
        if node.parent is None:
            self.root = new_root
        else:
            if node.parent.right == node:
                node.parent.right = new_root
            else:
                node.parent.left = new_root
        new_root.right = node
        node.parent = new_root
        node.height = max(self._get_height(node.left), self._get_height(node.right)) + 1
        new_root.height = max(self._get_height(new_root.left), self._get_height(new_root.right)) + 1
    
    def _rotate_left(self, node):
        new_root = node.right
        node.right = new_root.left
        if new_root.left is not None:
            new_root.left.parent = node
        new_root.parent = node.parent
        if node.parent is None:
            self.root = new_root
        else:
            if node.parent.left == node:
                node.parent.left = new_root
            else:
                node.parent.right = new_root
        new_root.left = node
        node.parent = new_root
        node.height = max(self._get_height(node.left), self._get_height(node.right)) + 1
        new_root.height = max(self._get_height(new_root.left), self._get_height(new_root.right)) + 1

    def _get_height(self, node):
        if node is None:
            return 0
        else:
            return node.height
    
    def _get_balance(self, node):
        if node is None:
            return 0
        else:
            return self._get_height(node.left) - self._get_height(node.right)

    