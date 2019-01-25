import json
from src import db
from flask_restful import Resource
from flask import request
from src.commons.date_utils import utc_timestamp
from src.commons.error_handler import http_basic_handler,RequestsError,requests_error_handler
from src.models.user import TbMerchant
from src.models.merchant_push import TbMerchantPush
from src.modules.push_strategy import push_conf
# model对应参数
merchant_push_fields={
    'merchant_code':{
        'to':'merchant_code',
        'required':True,
        'nullable':False,
        'type':str,
        'default':"",
    },
    'type':{
        'to':'type',
        'required':True,
        'nullable':False,
        'type':str,
        'default':"",
    },
    'push_method':{
        'to':'push_method',
        'required':True,
        'nullable':False,
        'type':str,
        'default':"",
    },
    'username':{
        'to':'username',
        'required':True,
        'nullable':False,
        'type':str,
        'default':"",
    },
    'password':{
        'to':'password',
        'required':True,
        'nullable':False,
        'type':str,
        'default':"",
    },
    'status':{
        'to':'status',
        'required':False,
        'nullable':False,
        'type':bool,
        'defatult':True,
    },
    'operator':{
        'to':'operator',
        'required':True,
        'nullable':False,
        'type':str,
        'default':"",
    },
    'operator_id':{
        'to':'operator_id',
        'required':True,
        'nullable':False,
        'type':str,
        'default':"",
    }
}
# 移除空值
def move_null(data):
    datas={}
    if isinstance(data,dict):
        for k,v in data.items():
            if v is not None:
                datas[k]=v
    return datas
# 移除空格
def move_space(data):
    datas={}
    if isinstance(data,dict):
        for k,v in data.items():
            datas[k.strip()]=v
    return datas

# 获取必需参数
def get_required():
    return [merchant_push_fields[k]['to'] for k,v in merchant_push_fields.items() if v['required']]

#校验参数类型
def check_type(data):
    for k,v in data.items():
        if k!='status' and type(v)!=merchant_push_fields[k]['type']:
            raise RequestsError(code=1014010001,message=",参数类型错误")
        if k=='status' and v not in [0,1]:
            raise RequestsError(code=1014010001, message=",status参数只接受整型数0，1")

# 获取查询参数
def get_pargrams(data,model):
    if isinstance(data,dict):
        queryset=model.query
        if data.get('merchant_code'):
            queryset = queryset.filter(model.merchant_code == data.get('merchant_code'))
        if data.get('operator'):
            queryset = queryset.filter(model.operator == data.get('operator'))
        if data.get('operator_id'):
            queryset = queryset.filter(model.operator_id == data.get('operator_id'))
        if data.get('type'):
            queryset = queryset.filter(model.type == data.get('type'))
        if data.get('push_method'):
            queryset = queryset.filter(model.push_method == data.get('push_method'))
        if data.get("status") is not None:
            queryset = queryset.filter(model.status == data.get('status'))
        if data.get('username'):
            queryset = queryset.filter(model.username == data.get('username'))
        if data.get('password'):
            queryset = queryset.filter(model.password == data.get('password'))
    else:
        queryset=None
    return queryset


class BaseMerchantPush(Resource):
    allowed_method=[]
    page=1
    default_page_size = 10
    max_page_size = 100
    # 目标model
    model=None
    #有默认值且需要的字段
    default_need_fields=[]
    # 推送方式
    push_methods=[]
    @requests_error_handler
    def dispatch_request(self, *args, **kwargs):
        if request.method not in self.allowed_method:
            raise RequestsError(code=1014000000, message=',请求方式错误')
        if request.method=='GET':
            merchant_code=request.args.get("merchant_code")
            data=request.args.get('data')
            type=request.args.get('type')
        else:
            merchant_code = request.json.get("merchant_code")
            data=request.json.get('data')
            type=request.json.get('type')
        if merchant_code and not TbMerchant.query.filter(TbMerchant.code==merchant_code).one_or_none():
            raise RequestsError(code=1014010005,message=",无此商户编号")

        if data:
            data=json.loads(data)
            type=data.get('type')
            if type and type not in push_conf.keys():
                raise RequestsError(code=1014010005,message=",无此推送类型")
            #判断是否有此推送方式
            if data.get('push_method') and data.get('push_method') not in self.push_methods:
                raise RequestsError(code=1014010005,message=",无此推送方式")
            #检测是否有多余的参数
            if set(data) - set(get_required() + self.default_need_fields):
                raise RequestsError(code=1014010004, message=",有未知参数")
            check_type(data)
        return super().dispatch_request(*args, **kwargs)
    @http_basic_handler
    def get(self):
        _id=request.args.get('id')
        operator=request.args.get('operator')
        operator_id=request.args.get('operator_id')
        if _id:
            instance=self.model.query.filter(self.model.id==_id).one_or_none()
            if not instance:
                raise RequestsError(code=1014010005,message=",查询数据时此id无对应数值")
            return instance.to_pretty()
        try:
            data=json.loads(request.args.get('data'))
        except:
            data={}
        data['merchant_code']=request.args.get('merchant_code')
        data['operator'] = operator
        data['operator_id'] = operator_id
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
            queryset=get_pargrams(move_space(data) ,self.model)
            if not queryset:
                return {
                    "total": 0,
                    "pages": 0,
                    "page": 0,
                    "page_size": 0,
                    "results": [],
                }
        else:
            queryset=self.model.query(self.model)
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

    @http_basic_handler
    def post(self):
        merchant_code=request.json.get('merchant_code')
        if not merchant_code:
            raise RequestsError(code=1014010001,message=",新增数据时商户编号缺失")
        try:
            data=json.loads(request.json.get('data'))
        except:
            data={}
        if not data:
            raise RequestsError(code=1014010001,message=",新增数据时缺少必需参数data或类型错误")
        data['operator_id']=request.json.get('operator_id')
        data['operator']=request.json.get('operator')
        data['merchant_code']=merchant_code
        data=move_null(data)
        # 判断是否必需参数都存在
        if set(get_required())-set(data):
            raise RequestsError(code=1014010001,message=",新增数据时必需参数缺失")
        #判断新增数据是否重复  merchant_code+push_type+push_method 三者唯一
        instance=self.model.query.filter(
            self.model.merchant_code==merchant_code,
            self.model.type==data.get('type'),
            self.model.push_method==data.get('push_method')).one_or_none()
        if instance:
            raise RequestsError(code=1014000000,message=",新增后唯一性数据重复")
        try:
            ins=self.model(**data)
            db.session.add(ins)
            db.session.commit()
        except:
            db.session.rollback()
            raise RequestsError(code=1014020001,message=",新增数据失败")
        return ins.to_pretty()

    @http_basic_handler
    def put(self):
        _id=request.json.get('id')
        if not _id:
            raise RequestsError(code=1014010001,message=",修改数据时需要必需参数id")
        instance=self.model.query.filter(self.model.id==_id).one_or_none()
        if not instance:
            raise RequestsError(code=1014010005,message=",修改数据时id对应数值不存在")
        operator=request.json.get('operator')
        operator_id=request.json.get('operator_id')
        if not all([operator,operator_id]):
            raise RequestsError(code=1014000000,message=",修改数据时无操作人信息")
        try:
            data=json.loads(request.json.get('data'))
        except:
            data={}
        if not data:
            # 如果没有data数据
            return instance.to_pretty()
        #如果有data数据 进行数据修改
        data['operator_id']=operator_id
        data['operator']=operator
        data=move_null(data)
        type=data.get('type')
        push_method=data.get('push_method')
        status=data.get('status')
        if not type:
            type=instance.type
        if not push_method:
            push_method=instance.push_method
        if status is None:
            status=instance.status
        # 检测修改后的数据 是否与数据库中有重复
        if instance.push_method!=push_method or instance.type!=type:
            ins=self.model.query.filter(
                self.model.merchant_code==instance.merchant_code,
                self.model.type==type,self.model.push_method==push_method).one_or_none()
            if ins:
                raise RequestsError(code=1014000000,message=",修改后唯一性数据重复")
        #修改数据
        try:
            instance.update_time=utc_timestamp()
            instance.type=type
            instance.push_method=push_method
            instance.status=status
            instance.operator=operator
            instance.operator_id=operator_id
            db.session.commit()
        except:
            # 事务回滚
            db.session.rollback()
            raise RequestsError(code=1014020001,message=",修改数据失败")
        return instance.to_pretty()
    @http_basic_handler
    def delete(self):
        _id=request.json.get('id')
        # 如果有id数值，直接删除此数据
        if _id:
            instance=self.model.query.filter(self.model.id==_id).one_or_none()
            if instance:
                try:
                    db.session.delete(instance)
                    db.session.commit()
                except:
                    db.session.rollback()
                return {}
        merchant_code=request.json.get('merchant_code')
        if not merchant_code:
            raise RequestsError(code=1014010001,message=",删除数据时id与商户编号至少需要一个")
        # 如果没有id 有merchant_code
        instances=self.model.query.filter(self.model.merchant_code==merchant_code).all()
        if instances:
            for instance in instances:
                try:
                    db.session.delete(instance)
                    db.session.commit()
                except:
                    db.session.rollback()
                    raise RequestsError(code=1014020001,message=",删除数据失败")
        return {}


class MerchantPush(BaseMerchantPush):
    allowed_method = ['GET', "POST", "PUT", 'DELETE']
    # 目标model
    model = TbMerchantPush
    # 有默认值且需要的字段
    default_need_fields = ['status']
    push_methods = ['http','stp']










