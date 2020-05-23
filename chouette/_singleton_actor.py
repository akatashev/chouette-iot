from pykka import ActorRegistry
from pykka.gevent import GeventActor


class SingletonActor(GeventActor):
    @classmethod
    def get_instance(cls):
        instances = ActorRegistry.get_by_class(cls)
        if instances:
            return instances.pop()
        return cls.start()
