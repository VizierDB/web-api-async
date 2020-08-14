# type: ignore
# flake8: noqa
'''
Created on Aug 5, 2020

@author: mike
'''

import sys, string
from types import *

if sys.version < '3':
    integer_types = (int, long,)
    string_types = (basestring,)
    split = string.split
else:
    integer_types = (int,)
    string_types = (str,)
    split = str.split

DICT_TYPES = {type(dict()): 1}
try:
    from BTree import BTree
    DICT_TYPES[BTree] = 1
except ImportError:
    pass
try:
    from BTrees import OOBTree, OIBTree, IOBTree
    DICT_TYPES[OOBTree.OOBTree] = 1
    DICT_TYPES[OIBTree.OIBTree] = 1
    DICT_TYPES[IOBTree.IOBTree] = 1
except ImportError:
    pass

# 
# IDEAS on how to restrict how deep we go when dumping:
#   - never follow cyclic links! (handled with 'seen' hash)
#   - have a maximum dumping depth (for any nestable structure:
#     list/tuple, dictionary, instance)
#   - if in an instance: don't dump any other instances
#   - if in an instance: don't dump instances that belong to classes from
#     other packages
#   - if in an instance: don't dump instances that belong to classes from
#     other modules
# ...ok, these are all implemented now -- cool!

# restrictions on dumping
max_depth = 5
instance_dump = 'module'                # or 'all', 'none', 'package':
                                        # controls whether we dump an
                                        # instance that is under another
                                        # instance (however deep)

# arg -- this is necessary because the .__name__ of a type object
# under JPython is a bit ugly (eg. 'org.python.core.PyList' not 'list')
TYPE_NAMES = {
    BuiltinFunctionType: 'builtin',
    BuiltinMethodType: 'builtin',
    CodeType: 'code',
    type(complex): 'complex',
    type(dict): 'dictionary',
    type(float): 'float',
    FrameType: 'frame',
    FunctionType: 'function',
    type(int): 'int',
    LambdaType: 'function',
    type(list): 'list',
    MethodType: 'instance method',
    ModuleType: 'module',
    type(None): 'None',
    type(string): 'string',
    TracebackType: 'traceback',
    type(tuple): 'tuple',
    type(type): 'type',
    }
try:
    TYPE_NAMES[ClassType] = 'class'
    TYPE_NAMES[DictType] = 'dictionary'
    TYPE_NAMES[DictProxyType] = 'dictionary'
    TYPE_NAMES[EllipsisType] = 'ellipsis'
    TYPE_NAMES[FileType] = 'file'
    TYPE_NAMES[InstanceType] = 'instance'
    TYPE_NAMES[LongType] = 'long int'
    TYPE_NAMES[SliceType] = 'slice'
    TYPE_NAMES[UnboundMethodType] = 'instance method'
    TYPE_NAMES[UnicodeType] = 'unicode',
    TYPE_NAMES[XRangeType] = 'xrange',
except NameError: pass


def get_type_name (some_type):
    try:
        return TYPE_NAMES[some_type]
    except KeyError:
        return some_type.__name__


class Dumper:

    def __init__ (self, max_depth=None, instance_dump=None, output=None):
        self._max_depth = max_depth
        self._instance_dump = instance_dump
        if output is None:
            self.out = sys.stdout
        else:
            self.out = output

    def __getattr__ (self, attr):
        if '_' + attr in self.__dict__:
            val = self.__dict__['_' + attr]
            if val is None:             # not defined in object;
                # attribute exists in instance (after adding _), but
                # not defined: get it from the module globals
                name_globals = vars(sys.modules[__name__])
                return name_globals.get(attr)
            else:
                # _ + attr exists and is defined (non-None)
                return val
        else:
            # _ + attr doesn't exist at all
            raise AttributeError(attr)


    def __setattr__ (self, attr, val):
        if '_' + attr in self.__dict__:
            self.__dict__['_' + attr] = val
        else:
            self.__dict__[attr] = val

    def _writeln (self, line):
        try:
            self.out.write(line.decode("utf-8"))
        except AttributeError:
            self.out.write(line) # python 3
        self.out.write(u"\n")
        
    def _write (self, text):
        try:
            self.out.write(text.decode("utf-8"))
        except AttributeError:
            self.out.write(text)

    def dump (self, val, indent='', summarize=1):
        self.seen = {}
        self.containing_instance = []
        self._dump (val, indent=indent, summarize=summarize)


    def _dump (self, val, depth=0, indent='', summarize=1):

        t = type (val)

        if short_value (val):
            self._write("%s%s" % (indent, short_dump (val)))

        else:
            depth = depth + 1

            if depth > self.max_depth:
                #raise SuppressedDump, "too deep"
                self._writeln(indent + "contents suppressed (too deep)")
                return

            if self.seen.get(id(val)):
                self._writeln(indent + "object already seen")
                return

            self.seen[id(val)] = 1

            if t in DICT_TYPES:
                if summarize:
                    self._writeln("%s%s:" % (indent, object_summary (val)))
                    indent = indent + '  '
                self.dump_dict (val, depth, indent)

            elif issubclass(t, (list, tuple)):
                if summarize:
                    self._writeln("%s%s:" % (indent, object_summary (val)))
                    indent = indent + '  '
                self.dump_sequence (val, depth, indent)

            elif is_instance(val):
                self.dump_instance (val, depth, indent, summarize)

            else:
                raise RuntimeError("this should not happen")

    # _dump ()


    def dump_dict (self, a_dict, depth, indent, shallow_attrs=()):
        keys = list(a_dict.keys())
        if type(keys) is type(list):
            keys.sort()

        for k in keys:
            val = a_dict[k]
            if short_value (val) or k in shallow_attrs:
                self._writeln("%s%s: %s" % (indent, k, short_dump (val)))
            else:
                self._writeln("%s%s: %s" % (indent, k, object_summary(val)))
                self._dump(val, depth, indent+'  ', summarize=0)


    def dump_sequence (self, seq, depth, indent):
        for i in range (len (seq)):
            val = seq[i]
            if short_value (val):
                self._writeln("%s%d: %s" % (indent, i, short_dump (val)))
            else:
                self._writeln("%s%d: %s" % (indent, i, object_summary(val)))
                self._dump(val, depth, indent+'  ', summarize=0)


    def dump_instance (self, inst, depth, indent, summarize=1):
        
        if summarize:
            self._writeln(indent + "%s " % object_summary (inst))
            indent = indent + '  '
        instance_dump = self.instance_dump

        # already dumping a containing instance, and have some restrictions
        # on instance-dumping?
        if self.containing_instance and instance_dump != 'all':

            previous_instance = self.containing_instance[-1]
            container_module = previous_instance.__class__.__module__
            container_package = (split (container_module, '.'))[0:-1]

            current_module = inst.__class__.__module__
            current_package = (split (current_module, '.'))[0:-1]

            #print "dumping instance contained in another instance %s:" % \
            #      previous_instance
            #print "  container module = %s" % container_module
            #print "  container package = %s" % container_package

            # inhibit dumping of all contained instances?
            if instance_dump == 'none':
                #raise SuppressedDump, "contained instance"
                self._writeln(
                    indent + "object contents suppressed (contained instance)")
                return

            # inhibit dumping instances from a different package?
            elif (instance_dump == 'package' and
                  current_package != container_package):
                #raise SuppressedDump, \
                #      "contained instance from different package"
                self._writeln(
                    indent + "object contents suppressed (instance from different package)")
                return

            # inhibit dumping instances from a different module?
            elif (instance_dump == 'module' and
                  current_module != container_module):
                #raise SuppressedDump, \
                #      "contained instance from different module"
                self._writeln(
                    indent + "object contents suppressed (instance from different module)")
                return

        # if in containing instance and have restrictions

        #self._writeln("")
        self.containing_instance.append (inst)
        shallow_attrs = getattr(inst, "_dump_shallow_attrs", [])
        self.dump_dict (vars (inst), depth, indent, shallow_attrs)
        del self.containing_instance[-1]


# end class Dumper


# -- Utility functions -------------------------------------------------

def atomic_type (t):
    return issubclass(t,
                      (None.__class__, float, complex)) \
                      or issubclass(t, integer_types) \
                      or issubclass(t, string_types)

def short_value (val):
    #if atomic_type (type (val)):
    #    return 1

    t = type(val)

    if (t not in DICT_TYPES and not isinstance(val, list) and
        not isinstance(val, tuple) and not is_instance(val)):
        return 1

    elif (isinstance(val, list) or isinstance(val, tuple)) and len (val) <= 10:
        for x in val:
            if not atomic_type (type (x)):
                return 0
        return 1

    elif t in DICT_TYPES and len (val) <= 5:
        for (k,v) in list(val.items()):
            if not (atomic_type (type (k)) and atomic_type (type (v))):
                return 0
        return 1

    else:
        return 0


def short_dump (val):
    if atomic_type(type(val)) or is_instance(val) or is_class(val):
        return repr(val)
    
    elif isinstance(val, (list, tuple)):
        if isinstance(val, list):
            retval = '['
        else:
            retval = '('
        if len(val):
            retval += short_dump(val[0])
            for item in val[1:]:
                retval += ", " + short_dump(item)
        if isinstance(val, list):
            retval += ']'
        else:
            retval += ')'
        
        return retval

    else:
        try:
            val = repr(val)
        except UnicodeError as err:
            val = "[got unicode error trying to represent value: " + str(err) +\
                ']'
        return object_summary (val) + ': ' + repr(val)
    
        

def object_summary (val):
    t = type (val)

    if is_instance(val):
        if hasattr(val, '__str__'):
            strform = ": " + str(val)
        else:
            strform = ""
        return "<%s at %x%s>" % (val.__class__.__name__, id (val), strform)

    elif is_class(val):
        return "<%s %s at 0x%x>" % (get_type_name(t), val.__name__, id (val))

    else:
        return "<%s at 0x%x>" % (get_type_name(t), id (val))
    

def is_instance (val):
    try:
        if type(val) is InstanceType:
            return 1
        # instance of extension class, but not an actual extension class
        elif (hasattr(val, '__class__') and
              hasattr(val, '__dict__') and
              not hasattr(val, '__bases__')):
            return 1
        else:
            return 0
    except NameError:  # Python > 3
        return (hasattr(val, '__class__') and \
              hasattr(val, '__dict__') and \
              not hasattr(val, '__bases__'))

def is_class (val):
    return hasattr(val, '__bases__')


default_dumper = Dumper()

def dump(val, output=None):
    if output is None:
        default_dumper.dump(val)
    else:
        Dumper(output=output).dump(val)

def dumps(val, *argv):
    from io import StringIO
    out = StringIO()
    dumper = Dumper(output=out)
    dumper.dump(val)
    for val in argv:
        dumper.dump(val)
    return out.getvalue()


if __name__ == "__main__":
    
    l1 = [3, 5, 'hello']
    t1 = ('uh', 'oh')
    l2 = ['foo', t1]
    d1 = {'k1': 'val1',
          'k2': l1,
          'k2': l2}

    print("a list: ", dumps (l1), "; a tuple: ", dumps (t1))
    print("a complex list: ")
    dump (l2)
    dump (d1)
    print("same dict, printed from dumps(): ")
    print(dumps(d1))
    dump (19)
    dump ("\nMy birth year!\n")

    dumper = Dumper (max_depth=1)
    l = ['foo', ['bar', 'baz', (1, 2, 3)]]
    dumper.dump (l)
    dumper.max_depth = 2
    dumper.dump (l)
    l[1][2] = tuple (range (11))
    dumper.dump (l)
    dumper.max_depth = None
    print(dumper.max_depth)
    
    class Foo: pass
    class Bar: pass

    f = Foo ()
    b = Bar ()

    f.a1 = 35
    f.a2 = l1
    f.a3 = l2
    f.b = b
    f.a4 = l2
    b.a1 = f
    b.a2 = None
    b.a3 = 'foo'

    dump (f)