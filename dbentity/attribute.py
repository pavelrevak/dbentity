import time as _time
import datetime as _datetime


class AttributeException(Exception):
    """General Data object error"""


class NumberOutOfRangeException(AttributeException):
    """If number is out of range"""


class WrongNumberFormatException(AttributeException):
    """If number is out of range"""


def last_time_to_string(secondsf):
    seconds = int(secondsf)
    minutes = int(seconds // 60)
    hours = int(minutes // 60)
    days = int(hours // 24)
    seconds %= 60
    minutes %= 60
    hours %= 24
    since_str = ''
    if days:
        since_str = f"{days}d {hours:02d}h {minutes:02d}m {seconds:02d}s"
    elif hours:
        since_str = f"{hours:02d}h {minutes:02d}m {seconds:02d}s"
    elif minutes:
        since_str = f"{minutes:02d}m {seconds:02d}s"
    elif seconds > 10:
        since_str = f"{secondsf:.1f}s"
    elif seconds > 1:
        since_str = f"{secondsf:.2f}s"
    elif seconds > .1:
        since_str = f"{secondsf:.3f}s"
    else:
        since_str = f"{secondsf / 1000:.1f}ms"
    return since_str


class Attribute():
    CREATE = True
    SAVE = True
    INDEX = False
    CONNECTION = False
    CONNECTIONS = False
    FUNCTION = None

    def __init__(
            self,
            name,
            db_key=None,
            form_key=None,
            default=None):
        self._name = name
        self._db_key = db_key
        self._form_key = form_key
        self._default = default

    def __repr__(self):
        return f'{self.__class__.__name__}:{self._name}'

    @property
    def name(self):
        return self._name

    @property
    def db_key(self):
        if self._db_key is None:
            self._db_key = self._name
        return self._db_key

    @property
    def form_key(self):
        return self._form_key

    @property
    def default(self):
        return self._default

    def is_name(self, name):
        return name == self._name

    def is_form_key(self, form_key):
        return form_key == self._form_key

    def to_template(self, value):
        if value is None:
            value = ''
        return value

    def from_form(self, value):
        if value is None and self._default is not None:
            value = self._default
        return value

    def to_value(self, value):
        return value

    def to_json(self, value):
        return value

    def from_value(self, value):
        return value


class IndexAttribute(Attribute):
    CREATE = False
    SAVE = False
    INDEX = True

    def __init__(
            self,
            name=None,
            db_key=None):
        if name is None:
            name = 'uid'
            if db_key is None:
                db_key = 'id'
        super().__init__(name, db_key=db_key)


class CreateIndexAttribute(IndexAttribute):
    CREATE = True


class DatetimeAttribute(Attribute):
    def to_json(self, value):
        if isinstance(value, _datetime.datetime):
            return {
                'datetime': value.strftime('%Y-%m-%d %H:%M:%S'),
                'datetime_short': value.strftime('%Y%m%d%H%M%S'),
                'timestamp': value.timestamp(),
            }
        return value

    def to_template(self, value):
        if isinstance(value, _datetime.datetime):
            return {
                'datetime': value.strftime('%Y-%m-%d %H:%M:%S'),
                'datetime_short': value.strftime('%Y%m%d%H%M%S'),
                'timestamp': value.timestamp(),
            }
        return value


class LastTimeAttribute(Attribute):
    def to_json(self, value):
        if isinstance(value, (int, float)):
            return _time.time() - value

    def to_value(self, value):
        if isinstance(value, (int, float)):
            return _time.time() - value

    def to_template(self, value):
        if isinstance(value, (int, float)):
            secondsf = _time.time() - value
            since_str = last_time_to_string(secondsf)
            return {
                'timestamp': value,
                'since_sec': secondsf,
                'since_str': since_str}
        return value


class MinLastTimeAttribute(Attribute):
    FUNCTION = 'MIN'


class MaxLastTimeAttribute(Attribute):
    FUNCTION = 'MAX'


class StringAttribute(Attribute):
    pass


class BytesAttribute(Attribute):
    def to_json(self, value):
        if value is None:
            return None
        return repr(bytes(value))

    def to_template(self, value):
        if value is None:
            return None
        return repr(bytes(value))


class PasswordAttribute(Attribute):
    def to_template(self, value):
        return ""


class BooleanAttribute(Attribute):
    def from_form(self, value):
        if value is None and self._default is not None:
            value = self._default
        return bool(value)


class IntegerAttribute(Attribute):
    def __init__(
            self,
            name,
            db_key=None,
            form_key=None,
            default=None,
            minimal=None,
            maximal=None):
        self._min = minimal
        self._max = maximal
        super().__init__(
            name,
            db_key=db_key,
            form_key=form_key,
            default=default)

    def from_form(self, value):
        if not value:
            return None
        if not value.lstrip('+-').isdigit():
            raise WrongNumberFormatException()
        value = int(value)
        if self._min is not None and value < self._min:
            raise NumberOutOfRangeException()
        if self._max is not None and value > self._max:
            raise NumberOutOfRangeException()
        return value


class SumIntegerAttribute(Attribute):
    FUNCTION = 'SUM'

    def __init__(
            self,
            name,
            db_key=None,
            form_key=None,
            default=None,
            minimal=None,
            maximal=None):
        self._min = minimal
        self._max = maximal
        super().__init__(
            name,
            db_key=db_key,
            form_key=form_key,
            default=default)


class IntegerArrayAttribute(Attribute):
    def __init__(
            self,
            name,
            db_key=None,
            form_key=None,
            default=None,
            minimal=None,
            maximal=None):
        self._min = minimal
        self._max = maximal
        super().__init__(
            name,
            db_key=db_key,
            form_key=form_key,
            default=default)

    def from_form(self, values):
        if not values:
            return None
        if not isinstance(values, (list, tuple)):
            values = [values]
        new_values = set()
        if values:
            for value in values:
                if not value.lstrip('+-').isdigit():
                    raise WrongNumberFormatException()
                new_value = int(value)
                if self._min is not None and new_value < self._min:
                    raise NumberOutOfRangeException()
                if self._max is not None and new_value > self._max:
                    raise NumberOutOfRangeException()
                new_values.add(new_value)
        return sorted(new_values)


class FixedPointAttribute(Attribute):
    def __init__(
            self,
            name,
            db_key=None,
            form_key=None,
            default=None,
            fp=0,
            minimal=None,
            maximal=None):
        self._fp = fp
        self._min = minimal
        self._max = maximal
        super().__init__(
            name,
            db_key=db_key,
            form_key=form_key,
            default=default)

    def from_form(self, value):
        if value is None:
            return None
        value = value.replace(',', '.')
        if not value.lstrip('+-').replace('.', '', 1).isdigit():
            raise WrongNumberFormatException()
        value = float(value)
        if self._min is not None and value < self._min:
            raise NumberOutOfRangeException()
        if self._max is not None and value > self._max:
            raise NumberOutOfRangeException()
        return round(value * 10 ** self._fp)

    def to_template(self, value):
        if value is None:
            return ''
        return value / 10 ** self._fp

    def to_value(self, value):
        if value is None:
            return None
        return value / 10 ** self._fp

    def from_value(self, value):
        if value is None:
            return None
        return value * 10 ** self._fp


class SumFixedPointAttribute(Attribute):
    FUNCTION = 'SUM'

    def __init__(
            self,
            name,
            db_key=None,
            form_key=None,
            default=None,
            fp=0,
            minimal=None,
            maximal=None):
        self._fp = fp
        self._min = minimal
        self._max = maximal
        super().__init__(
            name,
            db_key=db_key,
            form_key=form_key,
            default=default)

    def to_template(self, value):
        if value is None:
            return ''
        return value / 10 ** self._fp

    def to_value(self, value):
        if value is None:
            return None
        return value / 10 ** self._fp


class ConnectionAttribute(Attribute):
    SAVE = False
    CONNECTION = True

    def __init__(
            self,
            name,
            sub_entity=None,
            db_key=None,
            conn_key=None):
        self._sub_entity = sub_entity
        self._conn_key = conn_key
        super().__init__(name, db_key=db_key)

    @property
    def sub_entity(self):
        return self._sub_entity

    @property
    def db_key(self):
        if self._db_key is None:
            self._db_key = f'{self._name}_id'
        return self._db_key

    @property
    def conn_key(self):
        if self._conn_key is None:
            self._conn_key = 'id'
        return self._conn_key

    @property
    def save(self):
        return False


class SubElementsAttribute(Attribute):
    SAVE = False
    CONNECTIONS = True

    def __init__(
            self,
            name):
        super().__init__(name, db_key=False)

    @property
    def save(self):
        return False
