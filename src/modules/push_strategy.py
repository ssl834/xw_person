from flask_restful import Resource
from flask import request
import json
from src.commons.error_handler import requests_error_handler, RequestsError, http_basic_handler
from src.models.push_strategy import PushStrategy
from src.commons.date_utils import utc_timestamp

push_convention_fileds = {
    'type': {
        'to': 'push_type',
        'type': str,
        'required': True,
        'nullable': False,
        'default': '',
        'desc': '推送类型'
    },
    'sub_type': {
        'to': 'sub_type',
        'type': str,
        'required': True,
        'nullable': False,
        'default': '',
        'desc': '推送子类型'
    },
    "status": {
        "to": "status",
        "type": int,
        'required': False,
        "nullable": True,
        'default': 1,
        'desc': "推送状态"
    },
    'code': {
        'to': 'code',
        'type': str,
        'required': True,
        'nullable': False,
        'default': '',
        'desc': '推送编码'
    },
    'desc': {
        'to': 'desc',
        'type': str,
        'required': True,
        'nullable': False,
        'default': '',
        'desc': '推送描述'
    },
    "operator": {
        "to": "operator",
        "type": str,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "操作人"
    },
    "operator_id": {
        "to": "operator",
        "type": str,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "操作人id"
    }
}
# 推送配置
push_conf = {
    # 类型
    'antifraud': {
        # 子类型
        'device_info': {
            'code': {
                'nullable': True,
                'default': ''
            }
        },
        'user_behavior': {
            'code': {
                'nullable': True,
                'default': ''
            }
        },
        'rules': {
            'code': {
                'nullable': False,
                'default': ''
            }
        }
    },
    'credit': {
        'third_zx': {
            'code': {
                'nullable': False,
                'default': ''
            }

        },
        'rules': {
            'code': {
                'nullable': False,
                'default': ''
            }
        }

    },
    'user': {
        'user_info': {
            'code': {
                'nullable': True,
                'default': ''
            }
        }
    },
    'ODS': {
        'ods': {
            'code': {
                'nullable': True,
                'default': ''
            }
        }
    }
}


# 获得必需参数
def get_require_fileds():
    return [push_convention_fileds[k]['to'] for k, v in push_convention_fileds.items() if v['required']]


# 去除请求参数中的空格
def move_space(request_fileds):
    datas = {}
    if isinstance(request_fileds, dict):
        for k, v in request_fileds.items():
            datas[k.strip()] = v
    return datas


# 检测参数类型是否符合要求
def checkout_type(type_filed, request_fields):
    if type_filed and type(type_filed) != push_convention_fileds['type']['type']:
        raise RequestsError(code=1014010001, message=",type类型错误")
    if request_fields:
        for k, v in request_fields.items():
            if type(v) != push_convention_fileds[k]['type']:
                raise RequestsError(code=1014010001, message=",data参数中类型错误")


# 去除空值
def move_null_val(data):
    if isinstance(data, dict):
        return {k: v for k, v in data.items() if v is not None}


class BasePushStrategy(Resource):
    # 允许的请求方式
    allow_method = []
    # model
    model = None
    # 包含字段
    include_fileds = []
    # 不允许更新字段
    update_exclude_fileds = []
    page = 1
    default_page_size = 10
    max_page_size = 100
    push_strategy = {}

    @requests_error_handler
    def dispatch_request(self, *args, **kwargs):
        if request.method not in self.allow_method:
            raise RequestsError(code=1014000000, message=',请求方式错误')
        if request.method == "GET":
            type_filed = request.args.get('type')
            request_fields = request.args.get('data')
            if not request_fields:
                request_fields = {}
            else:
                request_fields = move_space(json.loads(request_fields))
        else:
            type_filed = request.json.get('type')
            data = request.json.get('data')
            if not data:
                request_fields = {}
            else:
                data = json.loads(data)
                request_fields = move_space(data)
                # 检测是否有多余参数 所有的字段加上 而额外包含的字段

        if type_filed and type_filed not in self.push_strategy.keys():
            raise RequestsError(code=1014010002, message=",无此推送类型")
        if set(request_fields) - set(self.include_fileds + list(push_convention_fileds.keys())):
            raise RequestsError(code=1014010003, message=",data中存在未知参数")
        # 检测参数数据类型
        checkout_type(type_filed, request_fields)
        return super().dispatch_request(*args, **kwargs)

    @http_basic_handler
    def get(self):
        _id = request.args.get('id')
        type_field = request.args.get('type')
        operator = request.args.get('operator')
        operator_id = request.args.get('operator_id')
        data = request.args.get('data')
        if _id:
            try:
                instance = self.model.objects.filter(id=_id).first()
            except:
                instance = None
            if not instance:
                raise RequestsError(code=1014010005, message=",id对应数值不存在")
            return instance.to_pretty()
        # 没有id值
        try:
            page = request.args.get('page')
            if page:
                page = int(page)
            else:
                page = 1
        except:
            raise RequestsError(code=1014010001, message=",页数只能为整数")
        try:
            page_size = request.args.get("page_size")
            if page_size:
                page_size = int(page_size)
            else:
                page_size = self.default_page_size
            if page_size > self.max_page_size:
                page_size = self.max_page_size
        except:
            raise RequestsError(code=1014010001, message=",每页显示数只能为整数")  # 每页显示数只能为整数
        # 转换data为字典并去除k
        try:
            data = move_space(json.loads(data))
        except:
            data = {}
        data['operator'] = operator
        data['operator_id'] = operator_id
        data['push_type'] = type_field
        # 去除字典中v为None的数值
        data = move_null_val(data)
        if data:

            queryset = self.model.objects.filter(**data)
            if not queryset:
                return {
                    "total": 0,
                    "pages": 0,
                    "page": 0,
                    "page_size": 0,
                    "results": [],
                }
        else:
            queryset = self.model.objects
        if page_size == -1:
            page_size = queryset.count()
        pagination = queryset.paginate(page=page, per_page=page_size)
        return {
            "total": pagination.total,
            "pages": pagination.pages,
            "page": pagination.page,
            "page_size": pagination.per_page,
            "results": [obj.to_pretty() for obj in pagination.items],
        }

    # 新增数据
    @http_basic_handler
    def post(self):
        type_field = request.json.get('type')
        if not type_field:
            raise RequestsError(code=1014010001, message=",新增数据时type数值缺失")
        try:
            data = json.loads(request.json.get('data'))
        except:
            raise RequestsError(code=1014010001, message=',必需参数data缺失或类型错误')
        operator = request.json.get('operator')
        operator_id = request.json.get('operator_id')
        if not data:
            raise RequestsError(code=1014010001, message=",新增数据data数值缺失")
        data['push_type'] = type_field
        data['operator'] = operator
        data['operator_id'] = operator_id
        # 去除空格 并 去除值为None的数据
        data = move_null_val(move_space(data))
        # 检测是否满足必需参数条件
        if set(get_require_fileds()) - set(data):
            raise RequestsError(code=1014010001, message=",新增数据时必需参数缺失")
        sub_type = data.get('sub_type')
        if sub_type not in self.push_strategy[type_field].keys():
            raise RequestsError(code=1014010002, message=',无此推送子类型')
        code = data.get('code')
        # 如果code为'' 并且对应的类型下的子类型下的code可以为空，
        if not code:
            if self.push_strategy[type_field][sub_type]['code']['nullable']:
                data['code'] = self.push_strategy[type_field][sub_type]['code']['default']
            else:
                raise RequestsError(code=1014010002, message=",新增数据时此类型中的子类型 code不能为空")

        instance = self.model.objects.filter(
            push_type=type_field, sub_type=sub_type, code=data.get('code')).first()
        if instance:
            raise RequestsError(code=1014000000, message=",新增时唯一性数据重复")
        try:
            instance = self.model(**data)
            instance.save()
        except:
            raise RequestsError(code=1014020001, message=",新增数据失败")
        # 返回新增的数据
        return instance.to_pretty()

    # 修改数据
    @http_basic_handler
    def put(self):
        _id = request.json.get('id')
        type_filed = request.json.get('type')
        operator = request.json.get('operator')
        operator_id = request.json.get('operator_id')
        try:
            data = json.loads(request.json.get('data'))
        except:
            data = {}
        if not _id:
            raise RequestsError(code=1014010001, message=",修改数据时必需参数id缺失")
        try:
            instance = self.model.objects.filter(id=_id).first()
        except:
            instance = None
        if not instance:
            raise RequestsError(code=1014010005, message=",修改数据id对应数值不存在")
        # 如果传递过来只有一个id 则直接将原有数据进行返回  无修改
        if not data:
            return instance.to_pretty()
        data['push_type'] = type_filed
        data['operator'] = operator
        data['operator_id'] = operator_id
        data = move_null_val(move_space(data))
        if data.get("type") and data.get("type") not in self.push_strategy.keys():
            raise RequestsError(code=1014010002, message=",修改数据时无此推送类型")
        if data.get('type'):
            type_filed = data.get('type')
        else:
            type_filed = instance.push_type
        if data.get('sub_type') and data.get('sub_type') not in self.push_strategy[type_filed].keys():
            raise RequestsError(code=1014010002, message=",修改数据时无此推送子类型")
        if data.get('sub_type'):
            sub_type = data.get('sub_type')
        else:
            sub_type = instance.sub_type
        # 检测是否有code 如果有则检测是否符合规范
        code = data.get('code')
        if not code:
            if  self.push_strategy[type_filed][sub_type]['code']['nullable']:
                data['code'] = self.push_strategy[type_filed][sub_type]['code']['default']
            else:
                raise RequestsError(code=1014010002, message=",修改数据时此类型中的子类型 code不能为空")
        # 检测是否有不允许更新的字段
        if set(self.update_exclude_fileds) - set(data):
            raise RequestsError(code=1014010004, message=",更新数据有不允许字段")

        # 检测是否会与数据库中的重复
        if instance.push_type!=type_filed or instance.sub_type!=sub_type or instance.code!=code:
            ins = self.model.objects.filter(
                push_type=type_filed, sub_type=sub_type, code=data.get('code')).first()
            if ins:
                raise RequestsError(code=1014000000, message=",修改后的数据唯一性重复")
        # 更新数据
        data['update_time'] = utc_timestamp()
        try:
            instance.update(**data)
            instance.save()
        except:
            raise RequestsError(code=1014020001, message=",更新数据失败")
        # 返回修改后的数据
        return self.model.objects.filter(id=_id).first().to_pretty()

    # 删除数据
    @http_basic_handler
    def delete(self):
        _id = request.json.get('id')
        if _id:
            try:
                # 防止传入的id值不符合mongo规范
                instance = self.model.objects.filter(id=_id).first()
            except:
                instance = None
            if not instance:
                raise RequestsError(code=1014010005, message=",删除数据时id对应数值不存在")
            instance.delete()
            return {}
        type_field = request.json.get('type')
        if not type_field:
            raise RequestsError(code=1014010001, message=",删除数据时id与type必须存在一个")
        # 如果传递的参数是type则可能进行造成批量删除的情况
        instances = self.model.objects.filter(push_type=type_field)
        for instance in instances:
            instance.delete()
        return {}


class PushMessStrategy(BasePushStrategy):
    """单条数据的增删改查"""
    model = PushStrategy
    allow_method = ["GET", 'POST', 'PUT', 'DELETE']
    # 包含字段
    include_fileds = ['id', 'page', 'page_size']
    # 不允许更新字段
    update_exclude_fileds = []
    push_strategy = push_conf


class PushStrategyBatch(BasePushStrategy):
    """批量数据操作"""
    model = PushStrategy
    allow_method = ['GET', "POST"]
    include_fileds = ['id', 'page', 'page_size']
    push_strategy = push_conf
    not_in_data = ['push_type', 'operator', 'operator_id']

    # 批量导出
    @http_basic_handler
    def get(self):
        queryset = self.model.objects
        # 数据库中没有数据
        if not queryset:
            return {
                "total": 0,
                "results": []
            }
        # 数据库中是否有数据
        type_filed = request.args.get('type')
        # 如果有type参数 则导出对应的type类型的数据
        if type_filed:
            return {
                "total": self.model.objects.filter(push_type=type_filed).count(),
                "results": [obj.to_pretty() for obj in self.model.objects.filter(push_type=type_filed)],
            }
        return {
            "total": queryset.count(),
            "results": [obj.to_pretty() for obj in queryset],
        }

    # 批量导入 导入成功删除原有
    @http_basic_handler
    def post(self):
        type_filed = request.json.get('type')
        operator = request.json.get('operator')
        operator_id = request.json.get('operator_id')
        if not type_filed:
            raise RequestsError(code=1014010001, message=",批量导入数据时type数值缺失")
        if not all([operator, operator_id]):
            raise RequestsError(code=1014010001, message=",批量导入数据时操作人信息缺失")
        # 获取批量数据
        try:
            datas = json.loads(request.json.get('data'))
        except:
            datas = {}
        if not datas:
            raise RequestsError(code=1014010001, message=",批量导入数据时缺少必需参数data")
        # 校验全部数据
        for data in datas:
            data = move_null_val(move_space(data))
            if set(get_require_fileds()) - set(list(data.keys()) + self.not_in_data):
                raise RequestsError(code=1014010001, message=",批量导入数据时必需参数缺失")
            sub_type = data.get('sub_type')
            if sub_type not in self.push_strategy[type_filed]:
                raise RequestsError(code=1014010005, message=",批量导入数据时无此子类型")
            code = data.get('code')
            if not code and not self.push_strategy[type_filed][sub_type]['code']['nullable']:
                raise RequestsError(code=1014010002, message=",批量导入数据时此类型中的子类型 code不能为空")
        flag = utc_timestamp()
        # 检验完成进行插入 更新数据
        for data in datas:
            data['push_type'] = type_filed
            data['operator'] = operator
            data['operator_id'] = operator_id
            sub_type = data.get('sub_type')
            if not data.get('code') and self.push_strategy[type_filed][sub_type]['code']['nullable']:
                data['code'] = self.push_strategy[type_filed][sub_type]['code']['default']
            instance = self.model.objects.filter(**data).first()
            # 如果已经存在，则此数据不再进行插入 只更改更新时间
            if instance:
                instance.update(update_time=utc_timestamp())
                instance.save()
                continue
            else:
                ins = self.model.objects.filter(push_type=type_filed, sub_type=sub_type, code=data.get('code')).first()
                if ins:
                    # 如果三者组合已经存在，则更新数据
                    data['update_time'] = utc_timestamp()
                    ins.update(**data)
                    ins.save()
                else:
                    # 没有原有记录 直接插入
                    try:
                        ins = self.model(**data)
                        ins.save()
                    except:
                        raise RequestsError(code=1014020001, message=",新增数据失败")
        # 删除原有数据
        for instance in self.model.objects:
            if instance.update_time < flag:
                instance.delete()
        return {}
