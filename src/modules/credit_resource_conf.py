# coding=utf-8
import json
import copy
from flask import request
from flask_restful import Resource
from src.models.args_credit import CreditAreaCoefficient
from src.commons.error_handler import requests_error_handler, RequestsError, http_basic_handler
from src.commons.date_utils import utc_timestamp



# 区域系数详细字段
area_coefficient_fileds = {
    "code": {
        "to": "code",
        "type": str,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "区域代码"
    },
    "address": {
        "to": "address",
        "type": str,
        'required': False,
        "nullable": True,
        'default': '',
        'desc': "详细地址"
    },
    "province": {
        "to": "province",
        "type": str,
        'required': False,
        "nullable": True,
        'default': '',
        'desc': "省份"
    },
    "city": {
        "to": "city",
        "type": str,
        'required': False,
        "nullable": True,
        'default': '',
        'desc': "城市"
    },
    "county": {
        "to": "county",
        "type": str,
        'required': False,
        "nullable": True,
        'default': '',
        'desc': "区"
    },
    "area_type": {
        "to": "area_type",
        "type": str,
        'required': False,
        "nullable": True,
        'default': '',
        'desc': "区域类型"
    },
    "coefficient": {
        "to": "coefficient",
        "type": float,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "系数"
    },
    "status": {
        "to": "status",
        "type": bool,
        'required': False,
        "nullable": False,
        'default': True,
        'desc': "开启状态"
    },
    "operator": {
        "to": "operator",
        "type": str,
        'required': False,
        "nullable": True,
        'default': '',
        'desc': "操作人"
    },
    "operator_id": {
        "to": "operator",
        "type": str,
        'required': False,
        "nullable": True,
        'default': '',
        'desc': "操作人id"
    }

}
# 通用授信 配置
TYPECONF = {
    "PE00003": {
        "to_model": CreditAreaCoefficient,
        "to_fields": area_coefficient_fileds
    }
}


# 移除参数中空格
def move_space(data):
    datas = {}
    if data and isinstance(data, dict):
        for k, v in data.items():
            datas[k.strip()] = v
        return datas
    return {}


# 校验必需字段
def check_need_fields(new_add_fields, must_have_fields):
    required_fields = []
    for k, v in must_have_fields.items():
        if v["required"]:
            required_fields.append(k)
    if not set(required_fields) - set(new_add_fields.keys()):
        return True



# 校验字段类型
def check_fields_type(fields_arg, request_fields, request_method):  # 检测请求数据以及请求数据类型是否符合规范
    if request_fields:
        for k, v in request_fields.items():
            if type(request_fields[k]).__name__ == int.__name__:
                request_fields[k] = float(request_fields[k])
            if not type(request_fields[k]).__name__ == fields_arg[k]["type"].__name__:
                return True
    elif not request_fields and request_method == "GET":
        return False


# 将除需要的数字型以及布尔型数据之外的数据转换成str
def convert_str(new_add_fields, exclude_fields):
    results = list(set(new_add_fields) - set(exclude_fields))
    for result in results:
        if new_add_fields[result] is not None:
            new_add_fields[result] = str(new_add_fields[result])
    return new_add_fields


# # 去除v为空的字段
def move_null_vlaue(data):
    datas={}
    for k, v in data.items():
        if not v:
            datas[k]=v
    return {k:v for k,v in data.items() if k not in datas}
class CreditResource(Resource):
    """通用授信"""
    page = 1
    default_page_size = 10
    max_page_size = 100
    type_conf = {}
    undate_exclude_fields = []
    add_alter_have_fields = []
    allow_req_method = []
    model = None
    exclude_fields = []

    @requests_error_handler
    def dispatch_request(self, *args, **kwargs):
        if request.method not in self.allow_req_method:
            raise RequestsError(code=1014000000, message=',请求方式错误')
        request_method = request.method
        if request.method == "GET":
            type_filed = request.args.get('type')
            request_fields = request.args.get('data')
            if not request_fields:
                request_fields = {}
            else:
                request_fields = move_space(json.loads(request_fields))
        else:
            type_filed = request.json.get('type')
            try:
                data = request.json
            except:
                raise RequestsError(code=1014010001, message=',必需参数data缺失')
            data = request.json.get('data')
            if not data:
                request_fields = {}
            else:
                request_fields = move_space(json.loads(data))
        if not type_filed:
            raise RequestsError(code=1014010001, message=",配置类型参数缺失")

        if type_filed in self.type_conf and isinstance(request_fields, dict):
            self.model = self.type_conf[type_filed]["to_model"]
            fields_args = self.type_conf[type_filed]["to_fields"]
            if set(request_fields.keys()) - set((list(fields_args.keys()) + self.undate_exclude_fields)):
                raise RequestsError(code=1014010004, message=",传入参数有未知字段")
            errors = check_fields_type(fields_args, request_fields, request_method)
            if errors:
                raise RequestsError(code=1014010001, message=",参数类型错误")
        else:
            raise RequestsError(code=1014010005, message=",配置类型参数无效")
        return super().dispatch_request(*args, **kwargs)

    @http_basic_handler
    def get(self):
        data = request.args.get('data')
        if not data:
            data = {}
        else:
            convert_data = json.loads(data)
            data = move_space(convert_data)
        operator = request.args.get('operator')
        operator_id = request.args.get('operator_id')

        _id = request.args.get("id")
        if _id:
            try:
                instance = self.model.objects.filter(id=_id).first()
            except:
                instance = None
            if instance:
                return instance.to_pretty()
            raise RequestsError(code=1014010005, message=",id对应数值不存在")
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
        if data:
            data['operator'] = operator
            data['operator_id'] = operator_id
            data = move_null_vlaue(data)
            queryset = self.model.objects.filter(**data)
            if not queryset:
                return {
                    "total": 0,
                    "pages": 0,
                    "page": 0,
                    "page_size": 0,
                    "results": [],
                }
        elif not data:
            data['operator'] = operator
            data['operator_id'] = operator_id
            data=move_null_vlaue(data)
            queryset = self.model.objects.filter(**data)
        else:
            # 只传递type数值
            queryset = self.model.objects
        if page_size==-1:
            page_size=queryset.count()
        pagination = queryset.paginate(page=page, per_page=page_size)
        return {
            "total": pagination.total,
            "pages": pagination.pages,
            "page": pagination.page,
            "page_size": pagination.per_page,
            "results": [obj.to_pretty() for obj in pagination.items],
        }


    @http_basic_handler
    def post(self):
        type_con = request.json.get('type')
        must_have_fields = self.type_conf[type_con]["to_fields"]
        if set(self.add_alter_have_fields) - set(request.json.keys()):
            raise RequestsError(code=1014010001, message=",必需字段缺失")
        operator = request.json.get('operator')
        operator_id = request.json.get('operator_id')
        datas = request.json.get('data')
        if not datas:
            raise RequestsError(code=1014010001, message=",新增数据无效")
        request_fields = move_space(json.loads(request.json.get('data')))
        if not request_fields:
            raise RequestsError(code=1014010001, message=",新增数据无效")
        request_fields['operator'] = operator
        request_fields['operator_id'] = operator_id
        if not check_need_fields(request_fields, must_have_fields):
            raise RequestsError(code=1014010001, message=",必需参数缺失")
        if not request_fields.get('code'):
            raise RequestsError(code=1014000000, message=",code数值不能为空")
        try:
            instance = self.model(**request_fields)
            instance.save()
        except:
            raise RequestsError(code=1014020001, message=",该记录已存在")
        return instance.to_pretty()

    @http_basic_handler
    def put(self):
        _id = request.json.get('id')
        if not _id:
            raise RequestsError(code=1014010001, message=",id参数缺失")
        try:
            instance = self.model.objects.filter(id=_id).first()
        except:
            instance = None
        if not instance:
            raise RequestsError(code=1014010005, message=",id对应数值不存在")
        if set(self.add_alter_have_fields) - set(request.json.keys()):
            raise RequestsError(code=1014010001, message=",必需字段缺失")
        operator = request.json.get('operator')
        operator_id = request.json.get('operator_id')
        datas = request.json.get('data')
        if not datas:
            return instance.to_pretty()
        datas = move_space(json.loads(datas))
        if not datas:
            raise RequestsError(code=1014000000, message=",data参数无数据")
        if not datas.get('code'):
            raise RequestsError(code=1014000000, message=",code数值不能为空")
        datas['operator'] = operator
        datas['operator_id'] = operator_id
        instance["update_time"] = utc_timestamp()
        if set(datas.keys()) & set(self.undate_exclude_fields):
            raise RequestsError(code=1014010004, message=",存在不允许更新的参数")
        try:
            instance.update(**datas)
            instance.save()
        except:
            raise RequestsError(code=1014020001, message=",更新数据失败")
        instance = self.model.objects.filter(id=_id).first()
        return instance.to_pretty()

    @http_basic_handler
    def delete(self):
        _id = request.json.get('id')
        if not _id:
            raise RequestsError(code=1014010001, message=",必需参数id缺失")
        try:
            instance = self.model.objects.filter(id=_id).first()
        except:
            instance = None
        if not instance:
            raise RequestsError(code=1014010005, message=",id对应的数值不存在")
        instance.delete()
        return {}


class AreaCoefficient(CreditResource):
    """授信区域系数增删改查"""
    model = None
    allow_req_method = ['GET', 'PUT', 'DELETE', 'POST']
    undate_exclude_fields = ['id']
    add_alter_have_fields = ['type', 'data', 'operator_id', 'operator']
    type_conf = TYPECONF
    exclude_fields = ['coefficient', 'status']


class AreaCoefficientUpDown(CreditResource):
    """授信区域系数批量导入导出"""
    model = None
    allow_req_method = ["GET", "POST"]
    add_alter_have_fields = ['type', 'data', 'operator_id', 'operator']
    type_conf = TYPECONF
    exclude_fields = ['coefficient', 'status']

    # 批量导出
    def get(self):
        queryset = self.model.objects
        if queryset:
            return {
                "total": queryset.count(),
                "results": [obj.to_pretty() for obj in queryset],
            }
        return {
            "total": 0,
            "results": []
        }

    @http_basic_handler
    def post(self):
        type_con = request.json.get('type')
        must_have_fields = self.type_conf[type_con]["to_fields"]
        if set(self.add_alter_have_fields) - set(request.json.keys()):
            raise RequestsError(code=1014010001, message=",必需字段缺失")
        operator = request.json.get('operator')
        operator_id = request.json.get('operator_id')
        datas = request.json.get('data')
        if not datas:
            raise RequestsError(code=1014010001, message=",data参数无数据")
        datas = json.loads(datas)
        if not isinstance(datas, list):
            raise RequestsError(code=1014010001, message=",data数值类型错误")  # 批量导入时需要保证数值类型
        code_list=[]
        # 对批量数据中的必需字段校验
        for data in datas:
            request_fields = move_space(data)
            if not request_fields:
                raise RequestsError(code=1014010001, message=",data数值缺失或类型错误")
            if not check_need_fields(request_fields, must_have_fields):
                raise RequestsError(code=1014010001, message=",导入数据中必需参数缺失")
            if not request_fields.get('code'):
                raise RequestsError(code=1014000000,message=",code数值不能为空")
            code_list.append(request_fields.get('code'))
        # 对code中的唯一性进行确认
        if len(code_list)!=len(set(code_list)):
            raise RequestsError(code=1014020001, message=",导入数据code有重复值")
        self.model.objects.delete()
        for data in datas:
            data['operator'] = operator
            data['operator_id'] = operator_id
            new_add_fields = convert_str(data, self.exclude_fields)
            instance = self.model(**new_add_fields)
            instance.save()

        return {}
