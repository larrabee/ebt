import inspect


def getfunctions(func):
    functions = [x[0] for x in inspect.getmembers(func, inspect.isfunction)]
    return functions
