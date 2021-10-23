class DualLinkedListItem:

    def __init__(self, owner = None):
        self.__next__ = self.__prev__ = self.__list__ = None
        if owner is not None:
            self.owner = owner

    def in_list(self) -> bool:
      return self.__list__ is not None

    def get_next(self):
        if self.__list__ is not None:
            return self.__next__

    def get_prev(self):
        if self.__list__ is not None:
            return self.__prev__

    def remove(self):
        if self.__list__ is not None:
            self.__list__.remove(self)

class DualLinkedList:

    def __init__(self):
        self.first = self.last = None
        self.count = 0

    def add(self, item: DualLinkedListItem):
        if item.__list__ is not None:
            self.__list__.Remove(self)
        if self.first is None:
            self.first = item
            self.last = item
            item.__next__ = None
            item.__prev__ = None
        else:
            self.last.__next__ = item
            item.__prev__ = self.last
            item.__next__ = None
            self.last = item
        item.__list__ = self
        self.count += 1

    def add_first(self, item: DualLinkedListItem):
        if self.first is None:
            self.first = item
            self.last = item
            item.__next__ = None
            item.__prev__ = None
        else:
            self.first.__prev__ = item
            item.__next__ = self.first
            item.__prev__ = None
            self.first = item
        item.__list__ = self
        self.count += 1

    def clear(self):
        p = self.first;
        while p is not None:
            p2 = p
            p = p.__next__
            p2.__prev__ = None
            p2.__next__ = None
            p2.__list__ = None
        self.first = None
        self.last = None
        self.count = 0

    def reset(self):
        self.count = 0
        self.first = None
        self.last = None

    def remove(self, item: DualLinkedListItem):
        if item.__list__ is None: return
        if item.__next__ is None:
            if item.__prev__ is None:
                self.first = None
                self.last = None
            else:
                item.__prev__.__next__ = None
                self.last = item.__prev__
        else:
            if item.__prev__ is None:
                self.first = item.__next__
                self.first.__prev__ = None
            else:
                item.__next__.__prev__ = item.__prev__
                item.__prev__.__next__ = item.__next__
        self.count -= 1
        item.__list__ = None
        item.__prev__ = None
        item.__next__ = None

    def remove_first(self) -> DualLinkedListItem:
        Result = self.first
        if Result is None: return None
        if Result.__next__ is None:
            self.first = None
            self.last = None
        else:
            self.first = Result.__next__
            self.first.__prev__ = None
        self.count-= 1
        Result.__list__ = None
        Result.__prev__ = None
        Result.__next__ = None
        return Result

    def  insert_before(self, before: DualLinkedListItem, item: DualLinkedListItem):
        if before.__prev__ is None:
            self.add_first(item)
        else:
            before.__prev__.__next__ = item
            item.__prev__ = before.__prev__
            item.__next__ = before
            before.__prev__ = item
            item.__list__ = self
            self.count += 1

    def insert_after(self, after: DualLinkedListItem, item: DualLinkedListItem):
        if after.__next__ is None:
            self.add(item)
        else:
            after.__next__.__prev__ = item
            item.__next__ = after.__next__
            item.__prev__ = after
            after.__next__ = item
            item.__list__ = self
            self.count += 1
