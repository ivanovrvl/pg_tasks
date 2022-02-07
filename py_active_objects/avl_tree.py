# This code is under MIT licence, you can find the complete file here: https://github.com/ivanovrvl/pg_tasks/blob/main/LICENSE

class  TreeNode:

    def __init__(self, owner = None):
        self.__parent__ = self.__left__ = self.__right__ = None
        self.__balance__ = 0
        if owner is not None:
            self.owner = owner

    def get_successor(self):
        Result = self.__right__
        if Result is not None:
            while Result.__left__ is not None:
                Result = Result.__left__
        else:
            Result = self
            while (Result.__parent__ is not None) and (Result.__parent__.__right__ is Result):
                Result = Result.__parent__
            return Result.__parent__
        return Result

    def get_precessor(self):
        Result = self.__left__
        if Result is not None:
            while Result.__right__ is not None:
                Result = Result.__right__
        else:
            Result = self
            while (Result.__parent__ is not None) and (Result.__parent__.__left__ is Result):
                Result = Result.__parent__
            return Result.__parent__
        return Result

    def get_tree(self):
        if self.__parent__ is None:
            return None
        else:
            baseNode = self
            while baseNode.__parent__ is not None:
                baseNode = baseNode.__parent__
            return baseNode.owner

    def in_tree(self)->bool:
        return self.__parent__ is not None

    def remove(self):
        if self.__parent__ is not None:
            t = self.get_tree()
            t.remove(self);

class  Tree:

    def __init__(self, Comp):
        self.count= 0
        self.__base__ = TreeNode(self)
        self.__root__ = None
        self.__comp__ = Comp

    def __set_root__(self, Root: TreeNode):
        self.__root__ = Root
        self.__base__.__right__ = Root
        self.__base__.__left__ = Root
        if self.__root__ is not None:
            self.__root__.__parent__ = self.__base__

    def get_leftmost(self) -> TreeNode:
        Result = self.__root__
        if Result is None:
            return Result
        while Result.__left__ is not None:
            Result = Result.__left__
        return Result

    def get_rightmost(self) -> TreeNode:
        Result = self.__root__;
        if Result is None:
            return Result
        while Result.__right__ is not None:
            Result = Result.__right__
        return Result

    def __balance_after_delete__(self, node: TreeNode):
        while node is not None:
            if (node.FBalance == 1) or (node.FBalance == -1):
                return
            OldParent = node.__parent__
            if node.FBalance == 0:
                if OldParent is self.__base__:
                    return
                if OldParent.__left__ is node:
                    OldParent.FBalance += 1
                else:
                    OldParent.FBalance -= 1
                node = OldParent
            elif node.FBalance == 2:
                OldRight = node.__right__
                if OldRight.FBalance >= 0:
                    self.__rotate_left__(node)
                    node.FBalance = 1 - OldRight.FBalance
                    OldRight.FBalance -= 1
                    node = OldRight
                else:
                    OldRightLeft = OldRight.__left__
                    self.__rotate_right__(OldRight)
                    self.__rotate_left__(node)
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
                OldLeft = node.__left__
                if OldLeft.FBalance <= 0:
                    self.__rotate_right__(node)
                    node.FBalance = (-1 - OldLeft.FBalance)
                    OldLeft.FBalance += 1
                    node = OldLeft
                else:
                    OldLeftRight = OldLeft.__right__
                    self.__rotate_left__(OldLeft)
                    self.__rotate_right__(node)
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

    def __switch_position_with_successor__(self, node: TreeNode, ASuccessor: TreeNode):
        OldBalance = node.FBalance
        node.FBalance = ASuccessor.FBalance
        ASuccessor.FBalance = OldBalance

        OldParent = node.__parent__
        OldLeft = node.__left__
        OldRight = node.__right__
        OldSuccParent = ASuccessor.__parent__
        OldSuccLeft = ASuccessor.__left__
        OldSuccRight = ASuccessor.__right__

        if OldParent is not self.__base__:
            if OldParent.__left__ is node:
                OldParent.__left__ = ASuccessor
            else:
                OldParent.__right__ = ASuccessor
        else:
            self.__set_root__(ASuccessor)
        ASuccessor.__parent__ = OldParent

        if OldSuccParent is not node:
            if OldSuccParent.__left__ is ASuccessor:
                OldSuccParent.__left__ = node
            else:
                OldSuccParent.__right__ = node
            ASuccessor.__right__ = OldRight
            node.__parent__ = OldSuccParent
            if OldRight is not None:
                OldRight.__parent__ = ASuccessor
        else:
            ASuccessor.__right__ = node
            node.__parent__ = ASuccessor

        node.__left__ = OldSuccLeft
        if OldSuccLeft is not None:
            OldSuccLeft.__parent__ = node
        node.__right__ = OldSuccRight
        if OldSuccRight is not None:
            OldSuccRight.__parent__ = node
        ASuccessor.__left__ = OldLeft
        if OldLeft is not None:
            OldLeft.__parent__ = ASuccessor

    def __balance_after_insert__(self, node: TreeNode):
        OldParent = node.__parent__
        while OldParent is not self.__base__:
            if OldParent.__left__ is node:
                OldParent.FBalance -= 1
                if OldParent.FBalance == 0:
                    return
                if OldParent.FBalance == -1:
                    node = OldParent
                    OldParent = node.__parent__
                    continue;
                if node.FBalance == -1:
                    self.__rotate_right__(OldParent)
                    node.FBalance = 0
                    OldParent.FBalance = 0
                else:
                    OldRight = node.__right__
                    self.__rotate_left__(node)
                    self.__rotate_right__(OldParent)
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
                    OldParent = node.__parent__
                    continue
                if node.FBalance == 1:
                    self.__rotate_left__(OldParent);
                    node.FBalance = 0
                    OldParent.FBalance = 0
                else:
                    OldLeft = node.__left__
                    self.__rotate_right__(node)
                    self.__rotate_left__(OldParent)
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

    def __rotate_left__(self, node: TreeNode):
        OldRight = node.__right__;
        OldRightLeft = OldRight.__left__;
        AParent = node.__parent__;
        if AParent is not self.__base__:
            if AParent.__left__ is node:
                AParent.__left__ = OldRight
            else:
                AParent.__right__ = OldRight;
        else:
            self.__set_root__(OldRight)
        OldRight.__parent__ = AParent
        node.__parent__ = OldRight
        node.__right__ = OldRightLeft
        if OldRightLeft is not None:
            OldRightLeft.__parent__ = node
        OldRight.__left__ = node

    def __rotate_right__(self, node: TreeNode):
        OldLeft = node.__left__
        OldLeftRight = OldLeft.__right__
        AParent = node.__parent__;
        if AParent is not self.__base__:
            if AParent.__left__ is node:
                AParent.__left__ = OldLeft
            else:
                AParent.__right__ = OldLeft
        else:
            self.__set_root__(OldLeft)
        OldLeft.__parent__ = AParent
        node.__parent__ = OldLeft
        node.__left__ = OldLeftRight
        if OldLeftRight is not None:
            OldLeftRight.__parent__ = node
        OldLeft.__right__ = node

    def remove(self, node: TreeNode):
        if node.__parent__ is None:
            return
        if (node.__left__ is not None) and (node.__right__ is not None):
            self.__switch_position_with_successor__(node, node.get_successor())
        OldParent = node.__parent__
        node.__parent__ = None
        if node.__left__ is not None:
            Child = node.__left__
        else:
            Child = node.__right__
        if Child is not None:
            Child.__parent__ = OldParent
        if OldParent is not self.__base__:
            if OldParent.__left__ is node:
                OldParent.__left__ = Child
                OldParent.FBalance += 1
            else:
                OldParent.__right__ = Child
                OldParent.FBalance -= 1
            self.__balance_after_delete__(OldParent)
        else:
            self.__set_root__(Child)
        self.count -= 1

    def add(self, node: TreeNode, Comp = None):
        if Comp is None: Comp = self.__comp__
        if node.__parent__ is not None: node.remove()
        node.__left__ = None
        node.__right__ = None
        node.FBalance = 0
        self.count += 1
        if self.__root__ is not None:
            InsertPos = self.__find_insert_pos__(node, Comp)
            InsertComp = Comp(node, InsertPos)
            node.__parent__ = InsertPos
            if InsertComp < 0:
                InsertPos.__left__ = node
            else:
                InsertPos.__right__ = node
            self.__balance_after_insert__(node)
        else:
            self.__set_root__(node)

    def __find_insert_pos__(self, node, Comp = None):
        if Comp is None: Comp = self.__comp__
        Result = self.__root__
        while Result is not None:
            c = Comp(node, Result)
            if c < 0:
                if Result.__left__ is not None:
                    Result = Result.__left__
                else:
                    return Result
            else:
                if Result.__right__ is not None:
                    Result = Result.__right__
                else:
                    return Result
        return Result

    def find_nearest(self, Data: TreeNode, Comp) -> TreeNode:
        if Comp is None: Comp = self.__comp__
        Result = self.__root__
        while Result is not None:
            c = Comp(Data, Result)
            if c == 0:
                return Result
            if c < 0:
                if Result.__left__ is not None:
                    Result = Result.__left__
                else:
                    return Result
            else:
                if Result.__right__ is not None:
                    Result = Result.__right__
                else:
                    return Result
        return Result

    def find(self, Data, Comp) -> TreeNode:
        if Comp is None: Comp = self.__comp__
        Result = self.__root__
        while Result is not None:
            c = Comp(Data, Result)
            if c == 0:
                return Result
            if c < 0:
                Result = Result.__left__
            else:
                Result = Result.__right__
        return Result

    def find_or_add(self, node: TreeNode, Comp = None) -> TreeNode:
        Result = None
        if Comp is None: Comp = self.__comp__
        if __root__ is not None:
            InsertPos = self.__root__
            while InsertPos is not None:
                InsertComp = Comp(node, InsertPos)
                if InsertComp < 0:
                    if InsertPos.__left__ is not None:
                        InsertPos = InsertPos.__left__
                    else:
                        break
                else:
                    if InsertComp == 0:
                        return InsertPos
                    if InsertPos.__right__ is not None:
                        InsertPos = InsertPos.__right__
                    else:
                        break
            InsertComp = Comp(node, InsertPos)
            node.FBalance = 0
            node.__left__ = None
            node.__right__ = None
            node.__parent__ = InsertPos
            if InsertComp < 0:
                InsertPos.__left__ = node
            else:
                InsertPos.__right__ = node
            self.__balance_after_insert__(node)
        else:
            node.FBalance = 0
            node.__left__ = None
            node.__right__ = None
            self.__set_root__(node)
        self.count += 1
        return Result

    def find_leftmost_ge(self, Data: TreeNode, Comp = None) -> TreeNode:
        if Comp is None: Comp = self.__comp__
        Result = None
        node = self.__root__
        while node is not None:
            n = Comp(Data, node)
            if n <= 0:
                Result = node
                node = node.__left__
            else:
                node = node.__right__
        return Result

    def find_rightmost_le(self, Data: TreeNode, Comp = None) -> TreeNode:
        if Comp is None: Comp = self.__comp__
        Result = None
        node = self.__root__
        while node is not None:
            n = Comp(Data, node)
            if n < 0:
                node = node.__left__
            else:
                Result = node
                node = node.__right__
        return Result

    def find_leftmost_eq(self, Data: TreeNode, Comp = None) -> TreeNode:
        if Comp is None: Comp = self.__comp__
        Result = None
        node = self.__root__
        while node is not None:
            n = Comp(Data, node)
            if n <= 0:
                if n == 0:
                    Result = node
                node = node.__left__
            else:
                node = node.__right__
        return Result

    def find_rightmost_eq(self, Data: TreeNode, Comp = None) -> TreeNode:
        if Comp is None: Comp = self.__comp__
        Result = None
        node = self.__root__
        while node is not None:
            n = Comp(Data, node)
            if n < 0:
                node = node.__left__;
            else:
                if n == 0:
                    Result = node;
                node = node.__right__
        return Result

    def for_each(self, func):

        def process_node(node: TreeNode):
            if node.__left__ is not None: process_node(node.__left__)
            if node.__right__ is not None: process_node(node.__right__)
            func(node)

        if self.__root__ is not None: process_node(self.__root__)

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



