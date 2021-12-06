# This code is under MIT licence, you can find the complete file here: https://github.com/ivanovrvl/pg_tasks/blob/main/LICENSE

import avl_tree
import linked_list
from datetime import datetime, timedelta

class ActiveObject:

    def __init__(self, controller, type_name=None, id=None, priority:int=0):
        self.t:datetime = None
        self.type_name = type_name
        self.id = id
        self.controller = controller
        self.__tree_by_t__ = avl_tree.TreeNode(self)
        self.__tree_by_id__ = avl_tree.TreeNode(self)
        self.__signaled__ = linked_list.DualLinkedListItem(self)
        self.priority = priority
        if id is not None:
            controller.__tree_by_id__.add(self.__tree_by_id__)

    def process(self):
        pass

    def process_internal(self):
        self.process()

    def is_signaled(self) -> bool:
        return self.__signaled__.in_list()

    def is_scheduled(self) -> bool:
        return self.__tree_by_t__.in_tree()

    def schedule(self, t:datetime):
        if t is not None:
            if not self.__tree_by_t__.in_tree() or t < self.t:
                self.controller.__tree_by_t__.remove(self.__tree_by_t__)
                self.t = t
                self.controller.__tree_by_t__.add(self.__tree_by_t__)

    def schedule_delay(self, delay:timedelta):
        t = self.controller.now() + delay
        self.schedule(t)
        return t

    def schedule_milliseconds(self, delay):
        return self.schedule_delay(timedelta(milliseconds=delay))

    def schedule_seconds(self, delay):
        return self.schedule_delay(timedelta(seconds=delay))

    def schedule_minutes(self, delay):
        return self.schedule_delay(timedelta(minutes==delay))

    def unschedule(self):
        self.controller.__tree_by_t__.remove(self.__tree_by_t__)
        self.t = None

    def deactivate(self):
        self.controller.__tree_by_t__.remove(self.__tree_by_t__)
        self.t = None
        self.__signaled__.remove()

    def signal(self):
        if not self.__signaled__.in_list():
            self.controller.__signaled__[self.priority].add(self.__signaled__)

    def reached(self, t:datetime) -> bool:
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

    def now(self):
        return self.controller.now()

    def close(self):
        self.controller.__tree_by_t__.remove(self.__tree_by_t__)
        self.controller.__tree_by_id__.remove(self.__tree_by_id__)
        self.__signaled__.remove()

class ActiveObjectWithRetries(ActiveObject):

    def __init__(self, controller, type_name=None, id=None, priority:int=0):
        super().__init__(controller, type_name, id, priority)
        self.__next_retry__ = None
        self.__next_retry_interval__ = None
        self.min_retry_interval = 1
        self.max_retry_interval = 60

    def was_error(self):
        return self.__next_retry__ is not None

    def process_internal(self):
        try:
            if self.__next_retry__ is None \
            or self.reached(self.__next_retry__):
                super().process_internal()
                self.__next_retry__ = None
        except:
            if self.__next_retry__ is None:
                self.__next_retry_interval__ = self.min_retry_interval
            else:
                self.__next_retry_interval__ = self.__next_retry_interval__ + self.__next_retry_interval__
                if self.__next_retry_interval__ > self.max_retry_interval:
                    self.__next_retry_interval__ = self.max_retry_interval
            self.__next_retry__ = self.schedule_delay(timedelta(seconds=self.__next_retry_interval__))
            raise

class SignalPub:

    def __init__(self, owner=None):
        self.subscribers = linked_list.DualLinkedList()
        self.owner = owner

    def signal(self):
        item = self.subscribers.first
        while item is not None:
            sub = item.owner
            if not sub.edge or not sub.is_set:
                sub.is_set = True
                sub.owner.signal()
            item = item.next

    def close(self):
        item = self.subscribers.remove_first()
        while item is not None:
            sub = item.owner
            if not sub.edge or not sub.is_set:
                sub.is_set = True
                sub.owner.signal()
            item = self.subscribers.remove_first()


class SignalSub:

    def __init__(self, owner:ActiveObject, edge:bool=False, is_set=False, pub:SignalPub=None):
        self.owner = owner
        self.pub_link = linked_list.DualLinkedListItem(self)
        self.is_set = is_set
        self.edge = edge
        if pub is not None: self.subscribe(pub)

    def subscribe(self, pub:SignalPub):
        pub.subscribers.add(self.pub_link)

    def unsubscribe(self):
        self.pub_link.remove()

    def is_subscribed(self):
        return self.pub_link.in_list()

    def is_active(self):
        if self.is_set: return True
        if not self.pub_link.in_list(): return True
        return False

    def reset(self):
        res = self.is_active()
        self.is_set = False
        return res

    def close(self):
        self.unsubscribe()

class Flag:

    def __init__(self):
        self.__wait_up_queue__ = linked_list.DualLinkedList()
        self.__wait_down_queue__ = linked_list.DualLinkedList()
        self.__is_up__ = False

    def notify_all(self):
        if self.__is_up__:
            item = self.__wait_up_queue__.remove_first()
            while item is not None:
                item.owner.owner.signal()
                item = self.__wait_up_queue__.remove_first()
        else:
            item = self.__wait_down_queue__.remove_first()
            while item is not None:
                item.owner.owner.signal()
                item = self.__wait_down_queue__.remove_first()

    def notify(self) -> bool:
        if self.__is_up__:
            item = self.__wait_up_queue__.remove_first()
            if item is None: return False
            item.owner.owner.signal()
            return self.__wait_up_queue__.first is not None
        else:
            item = self.__wait_down_queue__.remove_first()
            if item is None: return False
            item.owner.owner.signal()
            return self.__wait_down_queue__.first is not None

    def up(self, notify_all:bool=True):
        if self.__is_up__: return False
        self.__is_up__ = True
        if notify_all:
            self.notify_all()

    def down(self, notify_all:bool=True):
        if not self.__is_up__: return False
        self.__is_up__ = False
        if notify_all:
            self.notify_all()

class FlagListener:

    def __init__(self, owner:ActiveObject):
        self.__wait_queue__ = linked_list.DualLinkedListItem(self)
        self.owner:ActiveObject = owner

    def close(self):
        self.__wait_queue__.remove()
        self.owner:ActiveObject = None
        self.flag:Flag = None

    def is_up(self, flag: Flag) -> bool:
        if flag.__is_up__:
            if self.__wait_queue__.list is flag.__wait_up_queue__:
                flag.__wait_up_queue__.remove(self.__wait_queue__)
            return True
        else:
            if self.__wait_queue__.list is None \
            or self.__wait_queue__.list is not flag.__wait_up_queue__:
                flag.__wait_up_queue__.add(self.__wait_queue__)
            return False

    def is_down(self, flag: Flag) -> bool:
        if not flag.__is_up__:
            if self.__wait_queue__.list is flag.__wait_down_queue__:
                flag.__wait_down_queue__.remove(self.__wait_queue__)
            return True
        else:
            if self.__wait_queue__.list is None \
            or self.__wait_queue__.list is not flag.__wait_down_queue__:
                flag.__wait_down_queue__.add(self.__wait_queue__)
            return False

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

class ActiveObjectsController():

    def __init__(self, priority_count:int=1):
        self.__tree_by_t__ = avl_tree.Tree(__comp_t__)
        self.__tree_by_id__ = avl_tree.Tree(__comp_id__)
        self.__signaled__ = [linked_list.DualLinkedList() for i in range(0, priority_count)]
        self.terminated: bool = False
        self.emulated_time = None

    def find(self, type_name, id) -> ActiveObject:
        node = self.__tree_by_id__.find((type_name,id), __compkey_id__)
        if node is not None:
            return node.owner

    def now(self) -> datetime:
        if self.emulated_time is None:
            return datetime.now()
        else:
            return self.emulated_time

    def get_nearest(self) -> ActiveObject:
        node = self.__tree_by_t__.get_leftmost()
        if node is not None:
            return node.owner

    def process(self, on_before=None, on_success=None, on_error=None) -> datetime:

        def do(obj:ActiveObject):
            if on_before is not None:
                if on_before(obj):
                    return
            if on_error is None:
                obj.process_internal()
                if on_success is not None:
                    on_success(obj)
            else:
                try:
                    obj.process_internal()
                    if on_success is not None:
                        on_success(obj)
                except Exception as e:
                    on_error(obj, e)

        def remove_next_signaled() -> ActiveObject:
            for queue in self.__signaled__:
                item = queue.remove_first()
                if item is not None:
                    return item

        while not self.terminated:
            obj = self.get_nearest()
            next_time = None
            while obj is not None:
                dt = (obj.get_t() - self.now()).total_seconds()
                if dt > 0:
                    next_time = obj.get_t()
                    break
                next_task = obj.next()
                obj.unschedule()
                obj.signal()
                obj = next_task

            item = remove_next_signaled()
            if item is None:
                return next_time
            n = 10
            while item is not None:
                do(item.owner)
                n -= 1
                if n < 0: break
                if self.terminated: break
                item = remove_next_signaled()

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

    def terminate(self):
        self.terminated = True

def simple_loop(controller:ActiveObjectsController):
    import time
    controller.emulated_time = None
    while not controller.terminated:
        next_time = controller.process()
        if controller.terminated: return
        if next_time is not None:
            delta = (next_time - controller.now()).total_seconds()
            if delta > 0:
                time.sleep(delta)

def emulate_asap(controller:ActiveObjectsController, start_time:datetime):
    controller.emulated_time = start_time
    while not controller.terminated:
        controller.emulated_time = controller.process()
        if controller.terminated: return
        if controller.emulated_time is None:
            raise Exception('controller.emulated_time is None!')


