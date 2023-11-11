# This code is under MIT licence, you can find the complete file here: https://github.com/ivanovrvl/pg_tasks/blob/main/LICENSE

class  TreeNode:

    def __init__(self, owner = None):
        self._parent = self._left = self._right = None
        self.__balance = 0
        if owner is not None:
            self.owner = owner

    def get_successor(self):
        Result = self._right
        if Result is not None:
            while Result._left is not None:
                Result = Result._left
        else:
            Result = self
            while (Result._parent is not None) and (Result._parent._right is Result):
                Result = Result._parent
            return Result._parent
        return Result

    def get_precessor(self):
        Result = self._left
        if Result is not None:
            while Result._right is not None:
                Result = Result._right
        else:
            Result = self
            while (Result._parent is not None) and (Result._parent._left is Result):
                Result = Result._parent
            return Result._parent
        return Result

    def get_tree(self):
        if self._parent is None:
            return None
        else:
            baseNode = self
            while baseNode._parent is not None:
                baseNode = baseNode._parent
            return baseNode.owner

    def in_tree(self)->bool:
        return self._parent is not None

    def remove(self):
        if self._parent is not None:
            t = self.get_tree()
            t.remove(self);

class  Tree:

    def __init__(self, Comp):
        self.count= 0
        self.__base = TreeNode(self)
        self.__root = None
        self.__comp = Comp

    def __set_root(self, Root: TreeNode):
        self.__root = Root
        self.__base._right = Root
        self.__base._left = Root
        if self.__root is not None:
            self.__root._parent = self.__base

    def get_leftmost(self) -> TreeNode:
        Result = self.__root
        if Result is None:
            return Result
        while Result._left is not None:
            Result = Result._left
        return Result

    def get_rightmost(self) -> TreeNode:
        Result = self.__root;
        if Result is None:
            return Result
        while Result._right is not None:
            Result = Result._right
        return Result

    def __balance_after_delete(self, node: TreeNode):
        while node is not None:
            if (node.FBalance == 1) or (node.FBalance == -1):
                return
            OldParent = node._parent
            if node.FBalance == 0:
                if OldParent is self.__base:
                    return
                if OldParent._left is node:
                    OldParent.FBalance += 1
                else:
                    OldParent.FBalance -= 1
                node = OldParent
            elif node.FBalance == 2:
                OldRight = node._right
                if OldRight.FBalance >= 0:
                    self.__rotate_left(node)
                    node.FBalance = 1 - OldRight.FBalance
                    OldRight.FBalance -= 1
                    node = OldRight
                else:
                    OldRightLeft = OldRight._left
                    self.__rotate_right(OldRight)
                    self.__rotate_left(node)
                    if OldRightLeft.FBalance <= 0:
                        node.FBalance = 0
                    else:
                        node.FBalance = -1
                    if OldRightLeft.FBalance >= 0:
                        OldRight.FBalance = 0
                    else:
                        OldRight.FBalance = 1
                    OldRightLeft.FBalance = 0
                    node = OldRightLeft
            else:
                OldLeft = node._left
                if OldLeft.FBalance <= 0:
                    self.__rotate_right(node)
                    node.FBalance = (-1 - OldLeft.FBalance)
                    OldLeft.FBalance += 1
                    node = OldLeft
                else:
                    OldLeftRight = OldLeft._right
                    self.__rotate_left(OldLeft)
                    self.__rotate_right(node)
                    if OldLeftRight.FBalance >= 0:
                        node.FBalance = 0
                    else:
                        node.FBalance = 1;
                    if OldLeftRight.FBalance <= 0:
                        OldLeft.FBalance = 0
                    else:
                        OldLeft.FBalance = -1
                    OldLeftRight.FBalance = 0
                    node = OldLeftRight

    def __switch_position_with_successor(self, node: TreeNode, ASuccessor: TreeNode):
        OldBalance = node.FBalance
        node.FBalance = ASuccessor.FBalance
        ASuccessor.FBalance = OldBalance

        OldParent = node._parent
        OldLeft = node._left
        OldRight = node._right
        OldSuccParent = ASuccessor._parent
        OldSuccLeft = ASuccessor._left
        OldSuccRight = ASuccessor._right

        if OldParent is not self.__base:
            if OldParent._left is node:
                OldParent._left = ASuccessor
            else:
                OldParent._right = ASuccessor
        else:
            self.__set_root(ASuccessor)
        ASuccessor._parent = OldParent

        if OldSuccParent is not node:
            if OldSuccParent._left is ASuccessor:
                OldSuccParent._left = node
            else:
                OldSuccParent._right = node
            ASuccessor._right = OldRight
            node._parent = OldSuccParent
            if OldRight is not None:
                OldRight._parent = ASuccessor
        else:
            ASuccessor._right = node
            node._parent = ASuccessor

        node._left = OldSuccLeft
        if OldSuccLeft is not None:
            OldSuccLeft._parent = node
        node._right = OldSuccRight
        if OldSuccRight is not None:
            OldSuccRight._parent = node
        ASuccessor._left = OldLeft
        if OldLeft is not None:
            OldLeft._parent = ASuccessor

    def __balance_after_insert(self, node: TreeNode):
        OldParent = node._parent
        while OldParent is not self.__base:
            if OldParent._left is node:
                OldParent.FBalance -= 1
                if OldParent.FBalance == 0:
                    return
                if OldParent.FBalance == -1:
                    node = OldParent
                    OldParent = node._parent
                    continue;
                if node.FBalance == -1:
                    self.__rotate_right(OldParent)
                    node.FBalance = 0
                    OldParent.FBalance = 0
                else:
                    OldRight = node._right
                    self.__rotate_left(node)
                    self.__rotate_right(OldParent)
                    if OldRight.FBalance <= 0:
                        node.FBalance = 0
                    else:
                        node.FBalance = -1
                    if OldRight.FBalance == -1:
                        OldParent.FBalance = 1
                    else:
                        OldParent.FBalance = 0
                    OldRight.FBalance = 0
                return
            else:
                OldParent.FBalance += 1
                if OldParent.FBalance == 0:
                    return
                if OldParent.FBalance == 1:
                    node = OldParent
                    OldParent = node._parent
                    continue
                if node.FBalance == 1:
                    self.__rotate_left(OldParent);
                    node.FBalance = 0
                    OldParent.FBalance = 0
                else:
                    OldLeft = node._left
                    self.__rotate_right(node)
                    self.__rotate_left(OldParent)
                    if OldLeft.FBalance >= 0:
                        node.FBalance = 0
                    else:
                        node.FBalance = 1
                    if OldLeft.FBalance == 1:
                        OldParent.FBalance = -1
                    else:
                        OldParent.FBalance = 0
                    OldLeft.FBalance = 0
                return

    def __rotate_left(self, node: TreeNode):
        OldRight = node._right;
        OldRightLeft = OldRight._left;
        AParent = node._parent;
        if AParent is not self.__base:
            if AParent._left is node:
                AParent._left = OldRight
            else:
                AParent._right = OldRight;
        else:
            self.__set_root(OldRight)
        OldRight._parent = AParent
        node._parent = OldRight
        node._right = OldRightLeft
        if OldRightLeft is not None:
            OldRightLeft._parent = node
        OldRight._left = node

    def __rotate_right(self, node: TreeNode):
        OldLeft = node._left
        OldLeftRight = OldLeft._right
        AParent = node._parent;
        if AParent is not self.__base:
            if AParent._left is node:
                AParent._left = OldLeft
            else:
                AParent._right = OldLeft
        else:
            self.__set_root(OldLeft)
        OldLeft._parent = AParent
        node._parent = OldLeft
        node._left = OldLeftRight
        if OldLeftRight is not None:
            OldLeftRight._parent = node
        OldLeft._right = node

    def remove(self, node: TreeNode):
        if node._parent is None:
            return
        if (node._left is not None) and (node._right is not None):
            self.__switch_position_with_successor(node, node.get_successor())
        OldParent = node._parent
        node._parent = None
        if node._left is not None:
            Child = node._left
        else:
            Child = node._right
        if Child is not None:
            Child._parent = OldParent
        if OldParent is not self.__base:
            if OldParent._left is node:
                OldParent._left = Child
                OldParent.FBalance += 1
            else:
                OldParent._right = Child
                OldParent.FBalance -= 1
            self.__balance_after_delete(OldParent)
        else:
            self.__set_root(Child)
        self.count -= 1

    def add(self, node: TreeNode, Comp = None):
        if Comp is None: Comp = self.__comp
        if node._parent is not None: node.remove()
        node._left = None
        node._right = None
        node.FBalance = 0
        self.count += 1
        if self.__root is not None:
            InsertPos = self.__find_insert_pos(node, Comp)
            InsertComp = Comp(node, InsertPos)
            node._parent = InsertPos
            if InsertComp < 0:
                InsertPos._left = node
            else:
                InsertPos._right = node
            self.__balance_after_insert(node)
        else:
            self.__set_root(node)

    def __find_insert_pos(self, node, Comp = None):
        if Comp is None: Comp = self.__comp
        Result = self.__root
        while Result is not None:
            c = Comp(node, Result)
            if c < 0:
                if Result._left is not None:
                    Result = Result._left
                else:
                    return Result
            else:
                if Result._right is not None:
                    Result = Result._right
                else:
                    return Result
        return Result

    def find_nearest(self, Data: TreeNode, Comp) -> TreeNode:
        if Comp is None: Comp = self.__comp
        Result = self.__root
        while Result is not None:
            c = Comp(Data, Result)
            if c == 0:
                return Result
            if c < 0:
                if Result._left is not None:
                    Result = Result._left
                else:
                    return Result
            else:
                if Result._right is not None:
                    Result = Result._right
                else:
                    return Result
        return Result

    def find(self, Data, Comp) -> TreeNode:
        if Comp is None: Comp = self.__comp
        Result = self.__root
        while Result is not None:
            c = Comp(Data, Result)
            if c == 0:
                return Result
            if c < 0:
                Result = Result._left
            else:
                Result = Result._right
        return Result

    def find_or_add(self, node: TreeNode, Comp = None) -> TreeNode:
        Result = None
        if Comp is None: Comp = self.__comp
        if __root is not None:
            InsertPos = self.__root
            while InsertPos is not None:
                InsertComp = Comp(node, InsertPos)
                if InsertComp < 0:
                    if InsertPos._left is not None:
                        InsertPos = InsertPos._left
                    else:
                        break
                else:
                    if InsertComp == 0:
                        return InsertPos
                    if InsertPos._right is not None:
                        InsertPos = InsertPos._right
                    else:
                        break
            InsertComp = Comp(node, InsertPos)
            node.FBalance = 0
            node._left = None
            node._right = None
            node._parent = InsertPos
            if InsertComp < 0:
                InsertPos._left = node
            else:
                InsertPos._right = node
            self.__balance_after_insert(node)
        else:
            node.FBalance = 0
            node._left = None
            node._right = None
            self.__set_root(node)
        self.count += 1
        return Result

    def find_leftmost_ge(self, Data: TreeNode, Comp = None) -> TreeNode:
        if Comp is None: Comp = self.__comp
        Result = None
        node = self.__root
        while node is not None:
            n = Comp(Data, node)
            if n <= 0:
                Result = node
                node = node._left
            else:
                node = node._right
        return Result

    def find_rightmost_le(self, Data: TreeNode, Comp = None) -> TreeNode:
        if Comp is None: Comp = self.__comp
        Result = None
        node = self.__root
        while node is not None:
            n = Comp(Data, node)
            if n < 0:
                node = node._left
            else:
                Result = node
                node = node._right
        return Result

    def find_leftmost_eq(self, Data: TreeNode, Comp = None) -> TreeNode:
        if Comp is None: Comp = self.__comp
        Result = None
        node = self.__root
        while node is not None:
            n = Comp(Data, node)
            if n <= 0:
                if n == 0:
                    Result = node
                node = node._left
            else:
                node = node._right
        return Result

    def find_rightmost_eq(self, Data: TreeNode, Comp = None) -> TreeNode:
        if Comp is None: Comp = self.__comp
        Result = None
        node = self.__root
        while node is not None:
            n = Comp(Data, node)
            if n < 0:
                node = node._left;
            else:
                if n == 0:
                    Result = node;
                node = node._right
        return Result

    def for_each(self, func):

        def process_node(node: TreeNode):
            if node._left is not None: process_node(node._left)
            if node._right is not None: process_node(node._right)
            func(node)

        if self.__root is not None: process_node(self.__root)

    def iter(self, backward: bool = False):
        if backward:
            node = self.get_rightmost()
            while node is not None:
                yield node
                node = node.get_precessor()
        else:
            node = self.get_leftmost()
            while node is not None:
                yield node
                node = node.get_successor()



