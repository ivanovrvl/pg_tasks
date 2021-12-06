class DualLinkedListItem:

    def __init__(self, owner = None):
        self.next = self.prev = self.list = None
        if owner is not None:
            self.owner = owner

    def in_list(self) -> bool:
      return self.list is not None

    @DeprecationWarning
    def get_next(self):
        if self.list is not None:
            return self.next

    @DeprecationWarning
    def get_prev(self):
        if self.list is not None:
            return self.prev

    def remove(self):
        if self.list is not None:
            self.list.remove(self)

class DualLinkedList:

    def __init__(self):
        self.first = self.last = None
        self.count = 0

    def add(self, item: DualLinkedListItem):
        if item.list is not None:
            item.list.remove(item)
        if self.first is None:
            self.first = item
            self.last = item
            item.next = None
            item.prev = None
        else:
            self.last.next = item
            item.prev = self.last
            item.next = None
            self.last = item
        item.list = self
        self.count += 1

    def add_first(self, item: DualLinkedListItem):
        if self.first is None:
            self.first = item
            self.last = item
            item.next = None
            item.prev = None
        else:
            self.first.prev = item
            item.next = self.first
            item.prev = None
            self.first = item
        item.list = self
        self.count += 1

    def clear(self):
        p = self.first;
        while p is not None:
            p2 = p
            p = p.next
            p2.prev = None
            p2.next = None
            p2.list = None
        self.first = None
        self.last = None
        self.count = 0

    def reset(self):
        self.count = 0
        self.first = None
        self.last = None

    def remove(self, item: DualLinkedListItem):
        if item.list is None: return
        if item.next is None:
            if item.prev is None:
                self.first = None
                self.last = None
            else:
                item.prev.next = None
                self.last = item.prev
        else:
            if item.prev is None:
                self.first = item.next
                self.first.prev = None
            else:
                item.next.prev = item.prev
                item.prev.next = item.next
        self.count -= 1
        item.list = None
        item.prev = None
        item.next = None

    def remove_first(self) -> DualLinkedListItem:
        Result = self.first
        if Result is None: return None
        if Result.next is None:
            self.first = None
            self.last = None
        else:
            self.first = Result.next
            self.first.prev = None
        self.count-= 1
        Result.list = None
        Result.prev = None
        Result.next = None
        return Result

    def  insert_before(self, before: DualLinkedListItem, item: DualLinkedListItem):
        if before.prev is None:
            self.add_first(item)
        else:
            before.prev.next = item
            item.prev = before.prev
            item.next = before
            before.prev = item
            item.list = self
            self.count += 1

    def insert_after(self, after: DualLinkedListItem, item: DualLinkedListItem):
        if after.next is None:
            self.add(item)
        else:
            after.next.prev = item
            item.next = after.next
            item.prev = after
            after.next = item
            item.list = self
            self.count += 1
