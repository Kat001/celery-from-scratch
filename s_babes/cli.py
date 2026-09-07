import sys
from .core import say_hello

def main():
    name = sys.argv[1] if len(sys.argv) > 1 else "world"
    print(say_hello(name))