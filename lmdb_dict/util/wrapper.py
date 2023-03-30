import functools


class FuncArgsWrapper:
    """Wrap a callable such that a mapper function is applied to its
    arguments upon each invocation.

    """
    def __init__(self, mapper, func):
        # assign func's __module__, __name__, etc.
        # (but DON'T update __dict__)
        #
        # (also assigns __wrapped__)
        functools.update_wrapper(self, func, updated=())

        self._mapper_ = mapper

    def __repr__(self):
        return repr(self.__wrapped__)

    def _map_args_(self, *args, **kwargs):
        return self._mapper_(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        mapped = self._map_args_(*args, **kwargs)

        if mapped is not None:
            (args, kwargs) = mapped

        return self.__wrapped__(*args, **kwargs)

    def __get__(self, instance, owner=None):
        if instance is None:
            return UnboundArgsWrapper(self._mapper_, self.__wrapped__)

        return BoundArgsWrapper(self._mapper_, self.__wrapped__, instance)


class BoundArgsWrapper(FuncArgsWrapper):
    """Wrap a bound method such that a mapper function is applied to
    its non-self arguments upon each invocation.

    """
    def __init__(self, mapper, func, instance):
        super().__init__(mapper, func.__get__(instance, None))


class UnboundArgsWrapper(FuncArgsWrapper):
    """Wrap an unbound method such that a mapper function is applied to
    its non-self arguments upon each invocation.

    """
    def _map_args_(self, *args, **kwargs):
        (instance, *map_args) = args

        mapped = self._mapper_(*map_args, **kwargs)

        if mapped is not None:
            (args, kwargs) = mapped
            return ((instance, *args), kwargs)

        return (args, kwargs)


def _map_apply_first(func, value, *args, **kwargs):
    return ((func(value), *args), kwargs)


def apply_first(func):
    """Construct an argument mapper to apply the given argument-mapping
    function to the wrapped function's first argument only.

    Any remaining arguments are passed through.

    """
    return functools.partial(_map_apply_first, func)


def argument_decorator(mapper):
    """Construct a decorator to apply the given mapper function to the
    function's arguments upon each invocation.

    """
    return functools.partial(FuncArgsWrapper, mapper)
