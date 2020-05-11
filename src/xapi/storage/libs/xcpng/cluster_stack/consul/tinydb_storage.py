import consul
from tinydb.storages import Storage as _Storage_
from tinydb.database import StorageProxy as _StorageProxy_
from tinydb.database import DataProxy


class ConsulDict(object):

    def __init__(self, *args, **kwargs):
        self.__consul = consul.Consul()
        self.__prefix = ''
        self.update(*args, **kwargs)

    def set_kv_prefix(self, prefix):
        self.__prefix = prefix

    def update(self, E=None, **F):
        if E is not None:
            try:
                for k in E:
                    if isinstance(self.get(k), ConsulDict):
                        self[k].update(E[k])
                    else:
                        self[k] = (E[k])
            except:
                for (k, v) in E:
                    if isinstance(self.get(k), ConsulDict):
                        self[k].update(v)
                    else:
                        self[k] = v
        for k in F:
            if isinstance(self.get(k), ConsulDict):
                self[k].update(F[k])
            else:
                self[k] = F[k]

    def __setitem__(self, _key, value, prefix=''):
        if prefix == '':
            if self.__prefix != '':
                ind, val = self.__consul.kv.get(key=self.__prefix, separator='/')
                if val is not None:
                    self.__consul.kv.delete(key=self.__prefix, recurse=True)
                else:
                    self.__consul.kv.delete(key="%s%s%s" % (self.__prefix, prefix, _key), recurse=True)
            else:
                 self.__consul.kv.delete(key="%s%s%s" % (self.__prefix, prefix, _key), recurse=True)

        if isinstance(value, dict) or isinstance(value, ConsulDict):
            if len(value) > 0:
                for k, v in value.iteritems():
                    if prefix == '':
                        self.__setitem__(k, v, prefix="%s/" % _key)
                    else:
                        self.__setitem__(k, v, prefix="%s%s/" % (prefix, _key))
            else:
                self.__consul.kv.put(key="%s%s%s/" % (self.__prefix, prefix, _key), value=None)
        else:
            self.__consul.kv.put(key="%s%s%s" % (self.__prefix, prefix, _key), value=str(value))

    def __delitem__(self, _key):
        if self.__prefix != '':
            __key="%s%s" % (self.__prefix, _key)
        else:
            __key="%s" % _key

        ind, val1 = self.__consul.kv.get(key="%s/" % __key, keys=True, separator='/')
        ind, val2 = self.__consul.kv.get(key="%s" % __key, keys=True, separator='/')
        if val1 is None:
            if val2 is None:
                raise KeyError(_key)

        if val1 is not None:
            self.__consul.kv.delete(key=__key, recurse=True)
        elif val2 is not None:
            self.__consul.kv.delete(key=__key)

        index, keys = self.__consul.kv.get(key=self.__prefix, keys=True, separator='/')
        if keys == None:
            self.__consul.kv.put(key="%s/" % self.__prefix, value=None)

    def __getitem__(self, _key):
        if self.__prefix == '':
            index, _keys = self.__consul.kv.get(key="%s" % _key, keys=True, separator='/')
        else:
            index, _keys = self.__consul.kv.get(key="%s%s" % (self.__prefix, _key), keys=True, separator='/')

        if _keys == None:
            raise KeyError(_key)
        else:
            if _keys[0].endswith('/'):
                cdct = ConsulDict()
                cdct.set_kv_prefix(_keys[0])
                return cdct
            else:
                index, value = self.__consul.kv.get(key=_keys[0])
                val = None
                try:
                    val = eval(value['Value'])
                except:
                    val = value['Value']
                finally:
                    return val

    def __iter__(self):
        index, _keys = self.__consul.kv.get(key=self.__prefix, keys=True, separator='/')
        if _keys is not None:
            for _key in _keys:
                _key = _key.replace(self.__prefix, '').rstrip('/')
                try:
                    _key = eval(_key)
                except:
                    pass
                yield _key
        else:
            yield None

    def clear(self):
        self.__consul.kv.delete(key=self.__prefix, recurse=True)
        self.__consul.kv.put(key=self.__prefix, value=None)

    def keys(self):
        retkeys = []
        if self.__prefix != '':
            ind, val = self.__consul.kv.get(key=self.__prefix, separator='/')
            if val is not None:
                return retkeys

        index, _keys = self.__consul.kv.get(key=self.__prefix, keys=True, separator='/')

        if _keys is not None:
            for _key in _keys:
                if self.__prefix != '':
                    _key = _key.replace(self.__prefix, '').rstrip('/')
                    try:
                        _key = eval(_key)
                    except:
                        pass
                    retkeys.append(_key)
                else:
                    _key = _key.rstrip('/')
                    try:
                        _key = eval(_key)
                    except:
                        pass
                    retkeys.append(_key)
        return retkeys

    def values(self):
        values = []
        if self.__prefix != '':
            ind, val = self.__consul.kv.get(key=self.__prefix, separator='/')
            if val is not None:
                return values

        index, _keys = self.__consul.kv.get(key=self.__prefix, keys=True, separator='/')
        if _keys is not None:
            for _key in _keys:
                if _key.endswith('/'):
                    cdct = ConsulDict()
                    cdct.set_kv_prefix(_key)
                    values.append(cdct)
                else:
                    index, value = self.__consul.kv.get(key=_key)
                    val = None
                    try:
                        val = eval(value['Value'])
                    except:
                        val = value['Value']
                    finally:
                        values.append(val)
        return values

    def items(self):
        items = []
        if self.__prefix != '':
            ind, val = self.__consul.kv.get(key=self.__prefix, separator='/')
            if val is not None:
                return items

        index, _keys = self.__consul.kv.get(key=self.__prefix, keys=True, separator='/')
        if _keys is not None:
            for _key in _keys:
                if self.__prefix != '':
                    __key = _key.replace(self.__prefix, '')
                else:
                    __key = _key
                if _key.endswith('/'):
                    cdct = ConsulDict()
                    cdct.set_kv_prefix(_key)
                    items.append((__key.rstrip('/'), cdct))
                else:
                    index, value = self.__consul.kv.get(key=_key)
                    val = None
                    try:
                        val = eval(value['Value'])
                    except:
                        val = value['Value']
                    try:
                        __key = eval(__key)
                    except:
                        pass
                    items.append((__key, val))
        return items

    def iterkeys(self):
        return iter(self)

    def itervalues(self):
        for k in self:
            yield self[k]

    def iteritems(self):
        for k in self:
            try:
                k = eval(k)
            except:
                pass
            yield (k, self[k])

    def get(self, _key, default=None):
        if _key in self.keys():
            return self[_key]
        else:
            return default

    def has_key(self, _key):
        if _key in self.keys():
            return True
        else:
            return False

    def pop(self, _key, default=None):
        if _key in self.keys():
            result = self[_key]
            del self[_key]
            return result
        if default is None:
            raise KeyError(_key)
        return default

    def setdefault(self, _key, default=None):
        if _key in self.keys():
            return self[_key]
        self[_key] = default
        return default

    def popitem(self, last=True):
        if self.keys() == 0:
            raise KeyError('dictionary is empty')
        _key = next(reversed(self.keys()) if last else iter(self.keys()))
        value = self.pop(_key)
        return _key, value

    def __repr__(self):
        retval = '{'
        first = True
        for k, v in self.items():
            try:
                k = eval(k)
            except:
                pass
            try:
                v = eval(v)
            except:
                pass
            if first:
                retval += "%r: %r" % (k, v)
            else:
                retval += ", %r: %r" % (k, v)
            first = False
        retval += '}'
        return retval

    def __len__(self):
        return len(self.keys())

    def __reduce__(self):
        items = [[k, self[k]] for k in self]
        inst_dict = vars(self).copy()
        for k in vars(ConsulDict()):
            inst_dict.pop(k, None)
        if inst_dict:
            return (self.__class__, (items,), inst_dict)
        return self.__class__, (items,)

    def copy(self):
        return self.__class__(self)


class Storage(_Storage_):

    def __init__(self, path, default_db='00000000-0000-0000-0000-000000000000'):
        super(Storage, self).__init__()
        self.__memory = ConsulDict()
        self.__db_path = path
        self.__db_name = default_db

    def set_db_path(self, path):
        if path.startswith('/'):
            raise RuntimeError('Incorrect KV prefix')
        elif path.endswith('/'):
            raise RuntimeError('Incorrect KV prefix')
        else:
            self.__db_path = path

    def set_db_name(self, name):
        if name.startswith('/'):
            raise RuntimeError('Incorrect KV prefix')
        elif name.endswith('/'):
            raise RuntimeError('Incorrect KV prefix')
        else:
            self.__db_name = name

    def read(self):
        self.__memory.set_kv_prefix("%s/%s/" % (self.__db_path, self.__db_name))
        return self.__memory

    def write(self, data):
        pass

    def load(self, data):
        self.__memory.update(data)

    def is_loaded(self):
        if len(self.__memory.keys()) > 0:
            return True
        else:
            return False


class StorageProxy(_StorageProxy_):

    def read(self):
        raw_data = self._storage.read()

        if self._table_name in raw_data:
            return DataProxy(raw_data[self._table_name], raw_data)
#            return raw_data[self._table_name]
        else:
            raw_data.update({self._table_name: {}})
            return DataProxy(raw_data[self._table_name], raw_data)
#            return raw_data[self._table_name]

    def write(self, data):
        try:
            # Try accessing the full data dict from the data proxy
            raw_data = data.raw_data
        except AttributeError:
            # Not a data proxy, fall back to regular reading
            raw_data = self._storage.read()

        raw_data[self._table_name] = dict(data)
        pass

    def purge_table(self):
        try:
            data = self._storage.read()
            del data[self._table_name]
        except KeyError:
            pass