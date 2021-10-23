import avl_tree
import linked_list
from datetime import datetime

class ActiveObject:

    def __init__(self, controller, type_name = None, id = None):
        self.t:datetime = None
        self.type_name = type_name
        self.id = id
        self.controller = controller
        self.__tree_by_t__ = avl_tree.TreeNode(self)
        self.__tree_by_id__ = avl_tree.TreeNode(self)
        self.__signaled__ = linked_list.DualLinkedListItem(self)
        if id is not None:
            controller.__tree_by_id__.add(self.__tree_by_id__)

    def process(self):
        pass

    def is_signaled(self) -> bool:
        return self.__signaled__.in_list()

    def is_scheduled(self) -> bool:
        return self.__tree_by_t__.in_tree()

    def schedule(self, t:datetime):
        if t is not None:
            if not self.__tree_by_t__.in_tree() or t < self.t:
                self.t = t
                self.controller.__tree_by_t__.add(self.__tree_by_t__)

    def deactivate(self):
        self.controller.__tree_by_t__.remove(self.__tree_by_t__)
        self.t = None
        self.__signaled__.remove()

    def signal(self):
        if not self.__signaled__.in_list():
            self.controller.__signaled__.add(self.__signaled__)

    def check(self, t:datetime) -> bool:
        if t is None:
            return True
        else:
            if t <= self.controller.now():
                return True
            else:
                self.schedule(t)
                return False

    def get_t(self) -> datetime:
        return self.t

    def next(self):
        t = self.__tree_by_t__.get_successor()
        if t is not None:
            return t.owner

    def close(self):
        self.controller.__tree_by_t__.remove(self.__tree_by_t__)
        self.controller.__tree_by_id__.remove(self.__tree_by_id__)
        self.__signaled__.remove()

def __compkey_id__(k, n):
    if k[0] > n.owner.type_name:
        return 1
    elif k[0] < n.owner.type_name:
        return -1
    elif k[1] > n.owner.id:
        return 1
    elif k[1] == n.owner.id:
        return 0
    else:
        return -1

def __compkey_type__(k, n):
    if k > n.owner.type_name:
        return 1
    elif k < n.owner.type_name:
        return -1
    else:
        return 0

def __comp_id__(n1, n2):
    return __compkey_id__((n1.owner.type_name, n1.owner.id), n2)

def __comp_t__(n1, n2):
    if n1.owner.t > n2.owner.t:
        return 1
    elif n1.owner.t == n2.owner.t:
        return 0
    else:
        return -1

class ActiveObjectsController:

    def __init__(self):
        self.__tree_by_t__ = avl_tree.Tree(__comp_t__)
        self.__tree_by_id__ = avl_tree.Tree(__comp_id__)
        self.__signaled__ = linked_list.DualLinkedList()

    def find(self, type_name, id) -> ActiveObject:
        node = self.__tree_by_id__.find((type_name,id), __compkey_id__)
        if node is not None:
            return node.owner

    def now(self) -> datetime:
        return datetime.now()

    def get_nearest(self) -> ActiveObject:
        node = self.__tree_by_t__.get_leftmost()
        if node is not None:
            return node.owner

    def process(self, on_error=None) -> datetime:

        def do(obj:ActiveObject):
            if on_error is not None:
                obj.process()
            else:
                try:
                    obj.process()
                except Exception as e:
                    on_error(obj, e)

        while True:
            obj = self.get_nearest()
            next_time = None
            while obj is not None:
                dt = (obj.get_t() - self.now()).total_seconds()
                if dt > 0:
                    next_time = obj.get_t()
                    break
                next_task = obj.next()
                obj.deactivate()
                do(obj)
                obj = next_task
            item = self.__signaled__.remove_first()
            if item is None:
                return next_time
            n = 10
            while n > 0 and item is not None:
                do(item.owner)
                n -= 1
                item = self.__signaled__.remove_first()

    def for_each_object(self, type_name, func):
        n = self.__tree_by_id__.find_leftmost_eq(type_name, __compkey_type__)
        while n is not None and n.owner.type_name == type_name:
            func(n.owner)
            n = n.get_successor()

    def for_each_object_with_break(self, type_name, func):
        n = self.__tree_by_id__.find_leftmost_eq(type_name, __compkey_type__)
        while n is not None and n.owner.type_name == type_name:
            v = func(n.owner)
            if v:
                return v
            n = n.get_successor()
        return None

    def get_ids(self, type_name) -> list:
        res = list()
        self.for_each_object(type_name, lambda o: res.append(o.id))
        return res

    def signal(self, type_name):
        self.for_each_object(type_name, lambda o: o.signal())


