"""
Main Chouette module definition.
"""
from chouette._configuration import ChouetteConfig
from chouette._scheduler import Cancellable, Scheduler

__all__ = ["Cancellable", "Scheduler", "ChouetteConfig"]
