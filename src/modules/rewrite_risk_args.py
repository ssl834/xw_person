import random
import time

from flask import request
from src.commons.date_utils import utc_timestamp
from src.commons.logger import logger
from src.models.args_mgr import IndustryPosition
from src.models.risk_args import RiskArgs
from src.models.user import TbMerchant, TbProduction
from flask_restful import Resource
from src.commons.error_handler import (
    requests_error_handler, RequestsError, http_basic_handler,
)

degrees = {
    "EDU0001": "博士后",
    "EDU0002": "博士",
    "EDU0003": "研究生",
    "EDU0004": "大学本科",
    "EDU0005": "大专",
    "EDU0006": "大专以下"
}
rule_detail_conf = {
    "lend_range": {
        'to': 'lend_range',
        "type": list,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': " 放款区间"
    },
    'age_range': {
        'to': 'age_range',
        "type": list,
        'required': True,
        "nullable": True,
        'default': [],
        'desc': " 年龄范围"
    },
    'degrees': {
        'to': degrees,
        "type": list,
        'required': True,
        "nullable": True,
        'default': [],
        'desc': " 学历范围"

    },
    'selected_industries_and_professions': {
        'to': IndustryPosition,
        "type": list,
        'required': True,
        "nullable": True,
        'default': [],
        'desc': " 选择行业和职业"
    }

}
risk_args_conf = {
    "merchant_code": {
        "to": "merchant_code",
        "type": str,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "银行id"

    },
    "production_code": {
        "to": "production_code",
        "type": str,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "产品id"
    },
    "rule_name": {
        "to": "rule_name",
        "type": str,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "规则名称"
    },
    "rule_id": {
        "to": "rule_id",
        "type": int,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "规则id"
    },
    "rule_desc": {
        "to": "rule_desc",
        "type": str,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "规则描述"
    },
    "rule_conf": {
        "to": rule_detail_conf,
        "type": dict,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "规则配置"
    },
    "status": {
        "to": "status",
        "type": bool,
        'required': True,
        "nullable": False,
        'default': '',
        'desc': "操作开关"
    },
}


# 校验参数类型
def check_fields_type(datas, risk_conf):
    if isinstance(datas, dict):
        if datas:
            for k, v in datas.items():
                try:
                    if type(v).__name__ != risk_conf[k]['type'].__name__:
                        return False
                except:
                    raise RequestsError(code=1014010004, message=",id参数非必需")
        return True


# 校验参数字段
def check_fields(risk_args_conf):
    return [k for k, v in risk_args_conf.items() if v['required']]


# 将get请求中的参数转化类型
def get_convert_type(datas):
    if datas.get("status"):
        if str(datas['status']).lower() == "true":
            datas['status'] = True
        elif str(datas['status']).lower() == 'false':
            datas['status'] = False

        else:
            return False
    if datas.get('rule_id'):
        try:
            datas['rule_id'] = int(datas['rule_id'])
        except:
            return False
    return datas


# 移除get中page，page_size字段
def move_page_fields(datas, include_fields):
    dispose = {}
    for k, v in datas.items():
        if k not in include_fields:
            dispose[k] = v
    return dispose


# 移除空格
def move_space(datas):
    data = {}
    if datas:
        for k, v in datas.items():
            data[k.strip()] = v
        return data
    return {}


# 移除v为空的字段
def move_null_value(datas):
    return {k: v for k, v in datas.items() if v}


class ReWriteRiskArgs(Resource):
    model = None
    page = 1
    default_page_size = 10
    max_page_size = 100
    # 更新排除字段
    update_exclude_field = []
    allowed_request_methods = []
    include_fields = []

    @http_basic_handler
    @requests_error_handler
    def dispatch_request(self, *args, **kwargs):
        if request.method not in self.allowed_request_methods:
            raise RequestsError(code=1014000000, message=',请求方式错误')
        if request.method == "GET":
            request_fields = request.args.to_dict()
        else:
            try:
                # 校验除get之外的请求方式是否有数据
                request_fields = request.json
            except:
                raise RequestsError(code=1014010001, message=",必需参数缺失或类型错误")
        # 将参数字段去除空格
        request_fields = move_space(request_fields)
        #    校验参数是否符合要求
        if set(request_fields) - set(check_fields(risk_args_conf) + self.update_exclude_field + self.include_fields):
            raise RequestsError(code=1014010003, message=",未知参数存在")
        return super().dispatch_request(*args, **kwargs)

    @http_basic_handler
    def get(self):
        _id = request.args.get('id')
        if _id:
            try:
                instance = self.model.objects.filter(id=_id).first()
            except:
                instance = None
            if not instance:
                raise RequestsError(code=1014010005, message="记录不存在")
            return instance.to_dict()
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
        datas = request.args.to_dict()
        datas = move_page_fields(datas, self.include_fields)
        if datas:
            datas = get_convert_type(move_null_value(move_space(datas)))
            try:
                queryset = self.model.objects.filter(**datas)
            except:
                raise RequestsError(code=1014000000, message=",参数数值类型错误")
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
            "results": [obj.to_dict() for obj in pagination.items],
        }

    @http_basic_handler
    def delete(self):
        _id = request.json.get('id')
        if not _id:
            raise RequestsError(code=1014010001, message=",无此项记录")
        try:
            instance = self.model.objects.filter(id=_id).first()
        except:
            instance = None
        if not instance:
            raise RequestsError(code=1014010005, message=",id对应的数值不存在")
        instance.delete()
        return []


class ReWriteRiskArgsConf(ReWriteRiskArgs):
    model = RiskArgs
    update_exclude_field = ['merchant_code', 'production_code', 'rule_id', "created"]
    include_fields = ['id', 'page', 'page_size']
    allowed_request_methods = ["GET", "POST", "DELETE", "PUT"]

    @http_basic_handler
    def post(self):
        datas = move_null_value(move_space(request.json))
        if not check_fields_type(datas, risk_args_conf):
            raise RequestsError(code=1014010001, message=",参数类型错误")
        merchant_code = datas.get("merchant_code")
        production_code = datas.get('production_code')
        status = datas.get('status')
        if not all([merchant_code, production_code]):
            raise RequestsError(code=1014010001, message=',银行或产品编号缺失')
        if not TbMerchant.query.filter_by(code=merchant_code).first():
            raise RequestsError(code=1014010005, message=',商户编号不存在')
        if not TbProduction.query.filter_by(code=production_code, merchant_code=merchant_code).first():
            raise RequestsError(code=1014010005, message=',产品编号不存在')
        if status:
            instance = self.model.objects.filter(merchant_code=merchant_code, production_code=production_code,
                                                 status=True).first()
            if instance:
                raise RequestsError(code=1014000000, message=',现有规则已存在')
        rule_conf = datas.get('rule_conf')
        self.validate_rule_conf(rule_conf)
        try:
            instance = self.model(**datas)
        except:
            raise RequestsError(code=1014020001, message=",数据保存失败")
        instance.rule_id = int(str(int(time.time())) + str(random.randint(0, 9)))
        instance.save()
        return instance.to_dict()

    @http_basic_handler
    def put(self):
        _id = request.json.pop('id')
        if not _id:
            raise RequestsError(code=1014010001, message=',缺少必需字段id')
        try:
            instance = self.model.objects.filter(id=_id).first()
        except:
            instance = None
        if not instance:
            raise RequestsError(code=1014010005, message=",无此项记录")
        request_fields = request.json
        if set(request_fields) & set(self.update_exclude_field):
            raise RequestsError(code=1014010004, message=",有不允许更新字段")
        datas = move_null_value(move_space(request_fields))
        if not check_fields_type(datas, risk_args_conf):
            raise RequestsError(code=1014010001, message=",参数类型错误")
        merchant_code = datas.get("merchant_code")
        production_code = datas.get('production_code')
        status = datas.get('status')
        if status:
            instance = self.model.objects.filter(merchant_code=merchant_code, production_code=production_code,
                                                 status=True).first()
            if instance:
                raise RequestsError(code=1014000000, message=',现有规则已存在')
        rule_conf = datas.get('rule_conf')
        self.validate_rule_conf(rule_conf)
        try:
            instance.update(**datas)
        except:
            raise RequestsError(code=1014020001, message=",数据更新失败")
        instance.save()
        instance = self.model.objects.filter(id=_id).first()
        return instance.to_dict()

    @http_basic_handler
    def validate_rule_conf(self, rule_conf):
        # rule_conf中必需字段
        require_fields = check_fields(rule_detail_conf)
        # 请求中rule_conf字段
        request_rule_conf_fields = rule_conf.keys()
        if set(request_rule_conf_fields) - set(require_fields):
            raise RequestsError(code=1014010003, message=",不允许创建额外字段")
        if set(require_fields) - set(request_rule_conf_fields):
            raise RequestsError(code=1014010001, message=',必需字段缺失')
        # 检测rule_conf字段的类型
        if not check_fields_type(rule_conf, rule_detail_conf):
            raise RequestsError(code=1014010001, message=",规则参数类型错误")
        if not rule_conf["lend_range"]:
            raise RequestsError(code=1014000000, message=",放款区间不能为空")
        for rule_deg in rule_conf['degrees']:
            if rule_deg not in rule_detail_conf['degrees']['to'].keys():
                raise RequestsError(code=1014010005, message=',学历对应数值不存在')
        ind_pos_all = IndustryPosition.objects.all()
        industry_position_code = dict()
        for ind_pos in ind_pos_all:
            industry_position_code.setdefault(ind_pos.industry_code, set()).add(ind_pos.position_code)
        for ind_code in rule_conf['selected_industries_and_professions']:
            if ind_code['industry_code'] not in industry_position_code.keys():
                raise RequestsError(code=1014010005, message=',行业代码不存在')
            for prof in ind_code["professions"]:
                if prof["position_code"] not in industry_position_code[ind_code["industry_code"]]:
                    raise RequestsError(code=1014010005, message=',职位代码不存在')
