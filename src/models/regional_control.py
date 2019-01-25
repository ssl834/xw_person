from datetime import datetime
from src.commons.date_utils import utc_timestamp
from flask_mongoengine import BaseQuerySet
from mongoengine import (
    BooleanField,
    DateTimeField,
    IntField,
    Document,
    PolygonField,
    StringField,
)


class RegionalControl(Document):

    meta = {"collection": "support_regional_control", "ordering": ["-created"], "strict": False, "queryset_class": BaseQuerySet}

    production_code = StringField(required=True, verbose_name="产品code")
    created = IntField(required=True, default=utc_timestamp, verbose_name="创建时间")
    update_time = IntField(required=True, default=utc_timestamp, verbose_name="修改时间")
    status = BooleanField(required=True, default=False, verbose_name="状态")
    gps = PolygonField(required=True, verbose_name="坐标集")
    city = StringField(required=True, verbose_name="城市")
    address = StringField(required=True, verbose_name="地址")
    regional_type = StringField(required=True, verbose_name="区域类型")


def to_dict(instance):
    result = {}
    for field_name, field_type in instance._fields.items():
        if instance[field_name] is not None:
            if field_name == 'id':
                result[field_name] = str(instance[field_name])
            elif field_type.__class__ == DateTimeField:
                result[field_name] = int(instance[field_name].timestamp())
            else:
                result[field_name] = instance[field_name]
    return result


Document.to_dict = to_dict
