from pykka import ActorRegistry
from pykka.gevent import GeventActor


class SingletonActor(GeventActor):
    def __init__(self):
        super().__init__()
        self.name = self.__class__.__name__

    @classmethod
    def get_instance(cls):
        instances = ActorRegistry.get_by_class(cls)
        if instances:
            return instances.pop()
        return cls.start()
